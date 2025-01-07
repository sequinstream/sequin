defmodule Sequin.Postgres do
  @moduledoc false
  import Bitwise
  import Ecto.Query, only: [from: 2]

  alias Ecto.Type
  alias Sequin.Consumers.SourceTable
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Error.ValidationError
  alias Sequin.Repo

  require Logger

  @type db_conn() :: pid() | module() | DBConnection.t()

  @event_table_columns [
    %{name: "id", type: "serial"},
    %{name: "seq", type: "bigint"},
    %{name: "source_database_id", type: "uuid"},
    %{name: "source_table_oid", type: "bigint"},
    %{name: "source_table_schema", type: "text"},
    %{name: "source_table_name", type: "text"},
    %{name: "record_pk", type: "text"},
    %{name: "record", type: "jsonb"},
    %{name: "changes", type: "jsonb"},
    %{name: "action", type: "text"},
    %{name: "committed_at", type: "timestamp with time zone"},
    %{name: "inserted_at", type: "timestamp with time zone"}
  ]

  @doc """
  Checks if the given table is an event table by verifying it has all the required columns.
  """
  @spec is_event_table?(PostgresDatabaseTable.t()) :: boolean()
  def is_event_table?(%PostgresDatabaseTable{} = table) do
    required_column_names = MapSet.new(@event_table_columns, & &1.name)
    table_column_names = MapSet.new(table.columns, & &1.name)
    MapSet.subset?(required_column_names, table_column_names)
  end

  @doc """
  Compares the given table with the expected event table structure and returns a list of errors.
  """
  @spec event_table_errors(PostgresDatabaseTable.t()) :: [String.t()]
  def event_table_errors(%PostgresDatabaseTable{} = table) do
    table_columns = Map.new(table.columns, &{&1.name, &1})

    @event_table_columns
    |> Enum.reduce([], fn expected_column, errors ->
      case Map.get(table_columns, expected_column.name) do
        nil ->
          ["Missing column: #{expected_column.name}" | errors]

        actual_column ->
          if String.downcase(actual_column.type) == String.downcase(expected_column.type) do
            errors
          else
            [
              "Column type mismatch for #{expected_column.name}: expected #{expected_column.type}, got #{actual_column.type}"
              | errors
            ]
          end
      end
    end)
    |> Enum.reverse()
  end

  @doc """
  Executes a SQL query against a database connection.

  This function provides a unified interface for querying databases, supporting:
  - Direct Postgrex connections (when given a PID)
  - Module-based connections (when given a module name)
  - PostgresDatabase structs (using ConnectionCache)

  In test environments, this allows routing queries through Ecto's sandbox
  by caching Repo as the connection for test databases.

  ## Examples

      Postgres.query(conn_pid, "SELECT * FROM users")
      Postgres.query(Repo, "SELECT * FROM users")
      Postgres.query( postgres_database, "SELECT * FROM users")

  """
  def query(pid, query, params \\ [], opts \\ [])

  def query(pid, query, params, opts) when is_pid(pid) do
    Postgrex.query(pid, query, params, opts)
  end

  # When a transaction is open, this is the conn
  def query(%DBConnection{} = conn, query, params, opts) do
    Postgrex.query(conn, query, params, opts)
  end

  def query(mod, query, params, opts) when is_atom(mod) do
    mod.query(query, params, opts)
  end

  def query(%PostgresDatabase{} = db, query, params, opts) do
    case ConnectionCache.connection(db) do
      {:ok, conn_or_mod} ->
        case query(conn_or_mod, query, params, opts) do
          {:ok, result} ->
            {:ok, result}

          {:error, %Postgrex.Error{postgres: %{code: :undefined_column}}} = error ->
            DatabaseUpdateWorker.enqueue(db.id)
            error

          error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  def query!(pid, query, params \\ [], opts \\ []) do
    case query(pid, query, params, opts) do
      {:ok, %Postgrex.Result{} = result} -> result
      {:error, _} = error -> raise error
    end
  end

  @doc """
  Executes a transaction against a database connection.

  Similar to `query/4`, this function provides a unified interface for transactions, supporting:
  - Direct Postgrex connections (when given a PID)
  - Module-based connections (when given a module name)
  - PostgresDatabase structs (using ConnectionCache)

  ## Examples

      Postgres.transaction(conn_pid, fn conn -> ... end)
      Postgres.transaction(Repo, fn conn -> ... end)
      Postgres.transaction(postgres_database, fn conn -> ... end)
  """
  def transaction(conn, fun, opts \\ [])

  def transaction(pid, fun, opts) when is_pid(pid) do
    Postgrex.transaction(pid, fun, opts)
  end

  def transaction(mod, fun, opts) when is_atom(mod) do
    mod.transaction(fun, opts)
  end

  def transaction(%PostgresDatabase{} = db, fun, opts) do
    case ConnectionCache.connection(db) do
      {:ok, conn_or_mod} ->
        case transaction(conn_or_mod, fun, opts) do
          {:ok, result} -> {:ok, result}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  def get_major_pg_version(conn) do
    with {:ok, %{rows: [[version]]}} <- query(conn, "select version()") do
      # Extract major version number from version string
      case Regex.run(~r/PostgreSQL (\d+)/, version) do
        [_, major_version] -> {:ok, String.to_integer(major_version)}
        _ -> {:error, Error.service(message: "Could not parse PostgreSQL version", service: :postgres)}
      end
    end
  end

  def pg_type_to_ecto_type(pg_type) do
    case pg_type do
      "integer" -> :integer
      "bigint" -> :integer
      "smallint" -> :integer
      "text" -> :string
      "varchar" -> :string
      "char" -> :string
      "boolean" -> :boolean
      "float" -> :float
      "double precision" -> :float
      "numeric" -> :decimal
      "date" -> :date
      "timestamp" -> :naive_datetime
      "timestamptz" -> :utc_datetime
      "timestamp with time zone" -> :utc_datetime
      "timestamp without time zone" -> :naive_datetime
      "uuid" -> :binary_id
      "jsonb" -> :map
      "json" -> :map
      # Default to string for unknown types
      _ -> :string
    end
  end

  @spec pg_simple_type_to_filter_type(String.t()) :: SourceTable.filter_type()
  def pg_simple_type_to_filter_type(pg_type) do
    case pg_type do
      "smallint" -> :number
      "integer" -> :number
      "bigint" -> :number
      "boolean" -> :boolean
      "character varying" -> :string
      "text" -> :string
      "timestamp" <> _ -> :datetime
      "uuid" -> :string
      "numeric" -> :number
      "date" -> :datetime
      "jsonb" -> :jsonb
      "citext" -> :cistring
      _ -> :string
    end
  end

  def result_to_maps(%Postgrex.Result{} = result) do
    %{columns: columns, rows: rows} = result

    Enum.map(rows, fn row ->
      columns |> Enum.zip(row) |> Map.new()
    end)
  end

  # Helper function to cast values using Ecto's type system
  def cast_value(value, "uuid") do
    if Sequin.String.is_uuid?(value) do
      Sequin.String.string_to_binary!(value)
    else
      value
    end
  end

  def cast_value(value, pg_type) do
    ecto_type = pg_type_to_ecto_type(pg_type)

    case Type.cast(ecto_type, value) do
      {:ok, casted_value} ->
        casted_value

      :error ->
        Logger.warning("Failed to cast value #{inspect(value)} (pg_type: #{pg_type}) to ecto_type: #{ecto_type}")

        # Return original value if casting fails
        value
    end
  end

  def parameterized_tuple(count, offset \\ 0) do
    params = Enum.map_join(1..count, ", ", fn n -> "$#{n + offset}" end)
    "(#{params})"
  end

  @doc """
  Accepts a SQL string with ? placeholders. Returns a SQL string with $1, $2, etc. placeholders.
  """
  def parameterize_sql(sql) do
    sql
    |> String.split("?")
    |> Enum.with_index()
    |> Enum.reduce([], fn {part, index}, acc ->
      ["$#{index + 1}", part | acc]
    end)
    |> List.delete_at(0)
    |> Enum.reverse()
    |> Enum.join("")
  end

  def list_schemas(conn) do
    with {:ok, %{rows: rows}} <- query(conn, "SELECT schema_name FROM information_schema.schemata") do
      filtered_schemas =
        rows
        |> List.flatten()
        |> Enum.reject(&(&1 in ["pg_toast", "pg_catalog", "information_schema"]))

      {:ok, filtered_schemas}
    end
  end

  def fetch_tables_with_columns(conn, schemas) do
    schemas_list = Enum.map_join(schemas, ",", &"'#{&1}'")

    query = """
      select distinct on (n.nspname, c.relname, a.attnum)
        n.nspname as schema,
        c.relname as table_name,
        c.oid as table_oid,
        a.attnum,
        a.attname as column_name,
        pg_catalog.format_type(a.atttypid, -1) as column_type,
        coalesce(i.indisprimary, false) as is_pk,
        t.typtype as pg_typtype
      from pg_class c
      join pg_namespace n on c.relnamespace = n.oid
      join pg_attribute a on c.oid = a.attrelid
      join pg_type t on a.atttypid = t.oid
      left join pg_index i on c.oid = i.indrelid and a.attnum = any(i.indkey)
      where n.nspname in (#{schemas_list})
        and c.relkind in ('r', 'p')
        and a.attnum > 0
        and not a.attisdropped
        and not exists ( -- this excludes partition children
          select 1 from pg_inherits inh
          where inh.inhrelid = c.oid
        )
      order by n.nspname, c.relname, a.attnum, i.indisprimary desc nulls last
    """

    case query(conn, query) do
      {:ok, %{rows: rows}} ->
        tables = process_table_rows(rows)
        {:ok, tables}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Fetches unique indexes for a given table.

  ## Parameters
    - conn: The database connection
    - schema: The schema name
    - table: The table name

  ## Returns
    - {:ok, [%{name: String.t(), columns: [String.t()]}]} on success
    - {:error, Error.t()} on failure
  """
  @spec fetch_unique_indexes(DBConnection.conn() | PostgresDatabase.t(), String.t(), String.t()) ::
          {:ok, [%{name: String.t(), columns: [String.t()]}]} | {:error, Error.t()}
  def fetch_unique_indexes(conn, schema, table) do
    query = """
    select
      i.relname as index_name,
      array_agg(a.attname order by array_position(ix.indkey, a.attnum)) as column_names
    from
      pg_class t,
      pg_class i,
      pg_index ix,
      pg_attribute a,
      pg_namespace n
    where
      t.oid = ix.indrelid
      and i.oid = ix.indexrelid
      and a.attrelid = t.oid
      and a.attnum = any(ix.indkey)
      and t.relkind in ('r', 'p')
      and t.relname = $1
      and n.oid = t.relnamespace
      and n.nspname = $2
      and (ix.indisunique or ix.indisprimary)
    group by
      i.relname
    order by
      i.relname;
    """

    case query(conn, query, [table, schema]) do
      {:ok, %{rows: rows}} ->
        indexes =
          Enum.map(rows, fn [name, columns] ->
            %{name: name, columns: columns}
          end)

        {:ok, indexes}

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to fetch unique indexes",
           details: error
         )}
    end
  end

  defp process_table_rows(rows) do
    rows
    |> Enum.group_by(fn [schema, table_name, table_oid | _] -> {schema, table_name, table_oid} end)
    |> Enum.map(fn {{schema, table_name, table_oid}, columns} ->
      %PostgresDatabaseTable{
        oid: table_oid,
        schema: schema,
        name: table_name,
        columns:
          Enum.map(columns, fn [_, _, _, attnum, column_name, column_type, is_pk, pg_typtype] ->
            %PostgresDatabaseTable.Column{
              attnum: attnum,
              name: column_name,
              type: column_type,
              is_pk?: is_pk,
              pg_typtype: pg_typtype
            }
          end)
      }
    end)
  end

  @doc """
  Check if a replication slot (1) exists and (2) is currently busy or available.

  Returns a map like this:

      %{
        "slot_name" => slot_name,
        "slot_type" => slot_type,
        "database" => database,
        "active" => active
      }
  """
  @spec fetch_replication_slot(db_conn(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def fetch_replication_slot(conn, slot_name) do
    query = """
    select slot_name, active, database, slot_type
    from pg_replication_slots
    where slot_name = $1
    """

    with {:ok, res} <- query(conn, query, [slot_name]),
         [res] <- result_to_maps(res) do
      {:ok, res}
    else
      {:error, %Postgrex.Error{} = error} ->
        {:error, ValidationError.from_postgrex("Failed to check replication slot status: ", error)}

      [] ->
        {:error, Error.not_found(entity: :replication_slot, params: %{slot_name: slot_name})}
    end
  rescue
    error in [DBConnection.ConnectionError] ->
      {:error, Error.validation(summary: "Failed to check replication slot status: #{error.message}")}
  end

  def check_publication_exists(conn, publication_name) do
    query = "select 1 from pg_publication where pubname = $1"

    case query(conn, query, [publication_name]) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, Error.validation(summary: "Publication '#{publication_name}' does not exist")}

      {:error, %Postgrex.Error{} = error} ->
        {:error, ValidationError.from_postgrex("Failed to check publication: ", error)}
    end
  end

  def check_replication_permissions(conn) do
    query = """
    select pg_is_in_recovery(),
           current_setting('max_replication_slots')::int > 0,
           current_setting('wal_level') = 'logical'
    """

    case query(conn, query) do
      {:ok, %{rows: [[is_in_recovery, has_replication_slots, has_logical_wal_level]]}} ->
        cond do
          is_in_recovery ->
            {:error, Error.validation(summary: "Database is in recovery mode and cannot be used for replication")}

          not has_replication_slots ->
            {:error, Error.validation(summary: "Database does not have replication slots enabled")}

          not has_logical_wal_level ->
            {:error,
             Error.validation(
               summary:
                 "Database wal_level is not set to 'logical'. If you recently turned this setting on, it's possible you still need to restart your server."
             )}

          true ->
            :ok
        end

      {:error, %Postgrex.Error{} = error} ->
        {:error,
         ValidationError.from_postgrex(
           "Failed to check replication permissions: ",
           error
         )}
    end
  end

  @spec confirmed_flush_lsn(PostgresDatabase.t()) :: {:ok, integer()} | {:ok, nil} | {:error, Error.t()}
  def confirmed_flush_lsn(%PostgresDatabase{} = db) do
    query = """
    SELECT confirmed_flush_lsn
    FROM pg_replication_slots
    WHERE slot_name = $1
    """

    case query(db, query, [db.replication_slot.slot_name]) do
      {:ok, %{rows: [[lsn]]}} when not is_nil(lsn) ->
        {:ok, lsn_to_int(lsn)}

      {:ok, %{rows: [[nil]]}} ->
        {:ok, nil}

      {:ok, %{rows: []}} ->
        {:error, Error.not_found(entity: :replication_slot, params: %{slot_name: db.replication_slot})}

      {:error, _} = error ->
        error
    end
  end

  # In Postgres, an LSN is typically represented as a 64-bit integer, but it's sometimes split
  # into two 32-bit parts for easier reading or processing. We'll receive tuples like `{401, 1032909664}`
  # and we'll need to combine them to get the 64-bit LSN.
  @spec lsn_to_int(integer()) :: integer()
  @spec lsn_to_int(String.t()) :: integer()
  @spec lsn_to_int({integer(), integer()}) :: integer()
  def lsn_to_int(lsn) when is_integer(lsn), do: lsn

  def lsn_to_int(lsn) when is_binary(lsn) do
    [high, low] = lsn |> String.split("/") |> Enum.map(&String.to_integer(&1, 16))
    lsn_to_int({high, low})
  end

  def lsn_to_int({high, low}) do
    high <<< 32 ||| low
  end

  def list_tables(conn, schema) do
    with {:ok, %{rows: rows}} <-
           query(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", [schema]) do
      {:ok, List.flatten(rows)}
    end
  end

  def ecto_model_oid(model) do
    rel_name = model.__schema__(:source)

    Sequin.Repo.one(
      from(pg in "pg_class",
        where: pg.relname == ^rel_name,
        select: pg.oid
      )
    )
  end

  def fetch_table_oid(conn, schema, table) do
    case query(conn, "SELECT '#{quote_name(schema)}.#{quote_name(table)}'::regclass::oid") do
      {:ok, %{rows: [[oid]]}} -> oid
      _ -> nil
    end
  end

  def try_advisory_xact_lock(term) do
    lock_key = :erlang.phash2(term)

    case Repo.query("SELECT pg_try_advisory_xact_lock($1)", [lock_key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :locked}
    end
  end

  def check_replica_identity(conn, oid) do
    query = """
    SELECT relreplident
    FROM pg_class
    WHERE oid = $1;
    """

    case query(conn, query, [oid]) do
      {:ok, %{rows: [["f"]]}} ->
        {:ok, :full}

      {:ok, %{rows: [["d"]]}} ->
        {:ok, :default}

      {:ok, %{rows: [["n"]]}} ->
        {:ok, :nothing}

      {:ok, %{rows: [["i"]]}} ->
        {:ok, :index}

      {:error, error} ->
        Logger.error("Failed to check replica identity for oid #{oid}: #{inspect(error)}")
        {:error, error}
    end
  end

  @doc """
  For crafting a `select *`, but only selecting columns that are considered "safe"
  (i.e. we have a Postgrex encoder available)
  """
  def safe_select_columns(%PostgresDatabaseTable{} = table) do
    table.columns
    |> Enum.filter(&has_encoder?/1)
    |> Enum.map_join(", ", &quote_name(&1.name))
  end

  def sequence_nextval(sequence_name) do
    # Reason for casting explicitly: https://github.com/elixir-ecto/postgrex#oid-type-encoding
    with %{rows: [[val]]} <- Repo.query!("SELECT nextval($1::text::regclass)", [sequence_name]) do
      val
    end
  end

  def identifier(identifier) do
    identifier(identifier, [])
  end

  def identifier(prefix, identifier, suffix) do
    identifier(identifier, prefix: prefix, suffix: suffix)
  end

  def identifier(identifier, opts) when is_list(opts) do
    prefix = Keyword.get(opts, :prefix)
    suffix = Keyword.get(opts, :suffix)

    max_identifier_length =
      63 -
        if(prefix, do: byte_size(to_string(prefix)) + 1, else: 0) -
        if(suffix, do: byte_size(to_string(suffix)) + 1, else: 0)

    truncated_identifier = String.slice(identifier, 0, max_identifier_length)

    [prefix, truncated_identifier, suffix]
    |> Enum.reject(&is_nil/1)
    |> Enum.join("_")
  end

  def quote_names(names) do
    Enum.map_intersperse(names, ?,, &quote_name/1)
  end

  @doc """
  quote_name is vendored from Ecto.Adapters.Postgres.Connection.

  Used to create table names and column names in the form of "schema"."table".
  """
  def quote_name(nil, name), do: quote_name(name)

  def quote_name(prefix, name), do: to_string([quote_name(prefix), ?., quote_name(name)])

  def quote_name(name) when is_atom(name) do
    quote_name(Atom.to_string(name))
  end

  def quote_name(name) when is_binary(name) do
    if String.contains?(name, "\"") do
      {:error, "bad literal/field/index/table name #{inspect(name)} (\" is not permitted)"}
    end

    to_string([?", name, ?"])
  end

  @doc """
  Creates a logical replication slot if it doesn't already exist.
  """
  @spec create_replication_slot(DBConnection.conn(), String.t()) :: :ok | {:error, Error.t()}
  def create_replication_slot(conn, slot_name) do
    check_query = "select 1 from pg_replication_slots where slot_name = $1"

    case query(conn, check_query, [slot_name]) do
      {:ok, %{num_rows: 0}} ->
        create_query = "select pg_create_logical_replication_slot($1, 'pgoutput')::text"

        case query(conn, create_query, [slot_name]) do
          {:ok, _} ->
            :ok

          {:error, error} ->
            Sequin.Error.service(
              service: :postgres,
              message: "Failed to create replication slot",
              details: error
            )
        end

      {:ok, _} ->
        :ok

      {:error, error} ->
        Sequin.Error.service(
          service: :postgres,
          message: "Failed to check for existing replication slot",
          details: error
        )
    end
  end

  def tables_to_map(tables) do
    Enum.map(tables, fn table ->
      table
      |> Sequin.Map.from_ecto()
      |> Map.update!(:columns, fn columns ->
        Enum.map(columns, &Sequin.Map.from_ecto/1)
      end)
    end)
  end

  def load_rows(%PostgresDatabaseTable{} = table, rows) do
    Enum.map(rows, fn row ->
      Map.new(table.columns, fn col ->
        value = row[col.name]

        casted_val =
          cond do
            is_nil(value) ->
              nil

            # Binary data types
            col.type in ["bytea", "bit", "varbit"] and String.starts_with?(value, "\\x") ->
              value

            col.type in ["bytea", "bit", "varbit"] ->
              "\\x" <> Base.encode16(value, case: :lower)

            # UUID handling
            col.type == "uuid" ->
              Sequin.String.binary_to_string!(value)

            col.type in ["uuid[]", "_uuid"] and is_list(value) ->
              Enum.map(value, &Sequin.String.binary_to_string!/1)

            # This is the catch-all when encode is not implemented
            Jason.Encoder.impl_for(value) == Jason.Encoder.Any ->
              Logger.info("[Postgres] No Jason.Encoder for #{inspect(value)}", column: col.name, table: table.name)
              nil

            true ->
              value
          end

        {col.name, casted_val}
      end)
    end)
  end

  @safe_types [
    "array",
    "bigint",
    "bit",
    "bool",
    "boolean",
    "box",
    "bytea",
    "char",
    "character varying",
    "cidr",
    "circle",
    "citext",
    "date",
    "daterange",
    "double precision",
    "float4",
    "float8",
    "hstore",
    "inet",
    "int2",
    "int4",
    "int8",
    "integer",
    "interval",
    "json",
    "jsonb",
    "line",
    "lseg",
    "macaddr",
    "name",
    "numeric",
    "oid",
    "path",
    "point",
    "polygon",
    "real",
    "smallint",
    "text",
    "tid",
    "tsvector",
    "uuid",
    "varchar",
    "varbit",
    "void"
  ]

  # Catch-all for time formats
  defp has_encoder?(%PostgresDatabaseTable.Column{type: "time" <> _rest}) do
    true
  end

  # For composite types, enums, and domains, we don't yet store the underlying "base" type
  # For now, assume/hope that Postgrex has an encoder for the base type
  # In the future, we should store the base type in the column to determine if Postgrex can encode it
  defp has_encoder?(%PostgresDatabaseTable.Column{pg_typtype: typtype}) when typtype in ["c", "d", "e"] do
    true
  end

  defp has_encoder?(%PostgresDatabaseTable.Column{type: type} = column) do
    type = String.trim_trailing(type, "[]")

    if type in @safe_types do
      true
    else
      Logger.info("[Postgres] Unsupported column type (type=#{inspect(type)}, col=#{column.name})")
      false
    end
  end

  @doc """
  Checks if a table is part of a specified publication.

  ## Parameters
    - conn: The database connection
    - publication_name: The name of the publication to check
    - table_oid: The OID of the table to check
  """
  @spec verify_table_in_publication(DBConnection.conn() | PostgresDatabase.t(), String.t(), integer()) ::
          :ok | {:error, Error.ServiceError.t() | Error.NotFoundError.t()}
  def verify_table_in_publication(conn, publication_name, table_oid) do
    query = """
    select exists (
      select 1
      from pg_publication_tables pt
      join pg_class c on c.relname = pt.tablename
      join pg_namespace n on n.nspname = pt.schemaname and n.oid = c.relnamespace
      where pt.pubname = $1
        and (
          c.oid = $2  -- Direct match for regular tables
          or          -- OR
          exists (    -- Check for partition children
            select 1
            from pg_inherits i
            where i.inhrelid = c.oid
              and i.inhparent = $2
          )
        )
    )
    """

    case query(conn, query, [publication_name, table_oid]) do
      {:ok, %{rows: [[true]]}} ->
        :ok

      {:ok, %{rows: [[false]]}} ->
        {:error,
         Error.not_found(entity: :publication_membership, params: %{name: publication_name, table_oid: table_oid})}

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to check if table is in publication",
           details: error
         )}
    end
  end
end
