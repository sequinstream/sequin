defmodule Sequin.Postgres do
  @moduledoc false
  import Bitwise
  import Ecto.Query, only: [from: 2]
  import Sequin.Error.Guards

  alias Ecto.Type
  alias Sequin.Constants
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Error.ValidationError
  alias Sequin.Repo
  alias Sequin.WalPipeline.SourceTable

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
  @timeout_errors [
    "tcp recv: closed (the connection was closed by the pool, possibly due to a timeout or because the pool has been terminated)",
    "ssl recv: closed (the connection was closed by the pool, possibly due to a timeout or because the pool has been terminated)"
  ]
  @default_postgrex_timeout :timer.seconds(15)

  @doc """
  Checks if the given table is an event table by verifying it has all the required columns.
  """
  @spec event_table?(PostgresDatabaseTable.t()) :: boolean()
  def event_table?(%PostgresDatabaseTable{} = table) do
    required_column_names = MapSet.new(@event_table_columns, & &1.name)
    table_column_names = MapSet.new(table.columns, & &1.name)
    MapSet.subset?(required_column_names, table_column_names)
  end

  @doc """
  Determines the version of an event table based on its structure.

  Returns:
  - `:"v0"` if it's a basic event table without the 'transaction_annotations' column
  - `:"v3.28.25"` if it's an event table with the 'transaction_annotations' column
  """
  @spec event_table_version(PostgresDatabaseTable.t()) :: {:ok, :v0 | :"v3.28.25"} | {:error, Error.t()}
  def event_table_version(%PostgresDatabaseTable{} = table) do
    if event_table?(table) do
      has_transaction_annotations = Enum.any?(table.columns, &(&1.name == "transaction_annotations"))
      if has_transaction_annotations, do: {:ok, :"v3.28.25"}, else: {:ok, :v0}
    else
      {:error, Error.invariant(message: "Table is not an event table")}
    end
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

  # When a transaction is open, the conn is a DBConnection struct
  def query(pid_or_struct, query, params, opts) when is_pid(pid_or_struct) or is_struct(pid_or_struct, DBConnection) do
    case Postgrex.query(pid_or_struct, query, params, opts) do
      {:ok, result} ->
        {:ok, result}

      {:error, %DBConnection.ConnectionError{message: message}} when message in @timeout_errors ->
        timeout = Keyword.get(opts, :timeout, @default_postgrex_timeout)

        {:error,
         Error.service(
           message: "Query timed out",
           service: :postgres,
           details: %{timeout: timeout, query: query},
           code: :query_timeout
         )}

      {:error, error} ->
        {:error, error}
    end
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

  def get_pg_major_version(conn) do
    with {:ok, %{rows: [[version]]}} <- query(conn, "select version()") do
      # Extract major version number from version string
      case Regex.run(~r/PostgreSQL (\d+)/, version) do
        [_, major_version] -> {:ok, String.to_integer(major_version)}
        _ -> {:error, Error.service(message: "Could not parse PostgreSQL version", service: :postgres)}
      end
    end
  end

  # If you add a new type here, you may need to modify deserializers in changeset.ex
  # See: https://github.com/sequinstream/sequin/issues/1465
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
    if Sequin.String.uuid?(value) do
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

  def fetch_tables_with_columns(_conn, []) do
    {:error, Error.service(service: :postgres, message: "Fetch tables with columns: No schemas provided")}
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
  @spec get_replication_slot(db_conn(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def get_replication_slot(conn, slot_name) do
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
        {:error, Error.not_found(entity: :replication_slot, params: %{name: slot_name})}
    end
  rescue
    error in [DBConnection.ConnectionError] ->
      {:error, Error.validation(summary: "Failed to check replication slot status: #{error.message}")}
  end

  def check_replication_permissions(conn) do
    query = """
    select current_setting('max_replication_slots')::int > 0,
           current_setting('wal_level') = 'logical'
    """

    case query(conn, query) do
      {:ok, %{rows: [[has_replication_slots, has_logical_wal_level]]}} ->
        cond do
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

  @spec confirmed_flush_lsn(PostgresDatabase.t(), slot_name :: String.t()) ::
          {:ok, integer()} | {:ok, nil} | {:error, Error.t()}
  def confirmed_flush_lsn(%PostgresDatabase{} = db, slot_name) do
    query = """
    SELECT confirmed_flush_lsn
    FROM pg_replication_slots
    WHERE slot_name = $1
    """

    case query(db, query, [slot_name]) do
      {:ok, %{rows: [[lsn]]}} when not is_nil(lsn) ->
        {:ok, lsn_to_int(lsn)}

      {:ok, %{rows: [[nil]]}} ->
        {:ok, nil}

      {:ok, %{rows: []}} ->
        {:error,
         Error.not_found(
           entity: :replication_slot,
           params: %{name: slot_name}
         )}

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
      {:ok, %{rows: [[oid]]}} ->
        {:ok, oid}

      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} ->
        {:error, Error.not_found(entity: :table_oid, params: %{schema: schema, table: table})}

      {:error, error} ->
        {:error, error}
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
    select relreplident
    from pg_class
    where oid = $1;
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

      {:ok, %{rows: []}} ->
        {:error, Error.not_found(entity: :table, params: %{oid: oid})}

      {:error, error} ->
        Logger.error("Failed to check replica identity for oid #{oid}: #{inspect(error)}")
        {:error, error}
    end
  end

  @doc """
  Checks replica identity for a partitioned table and all its partitions.

  This function examines both the root table (provided by OID) and all of its partitions
  to determine if they have consistent replica identity settings.

  Returns:
  - `{:ok, :mixed}` if partitions have different replica identity settings
  - `{:ok, identity}` where identity is `:full`, `:default`, `:nothing`, or `:index` if all partitions have the same setting
  - `{:error, error}` on failure

  ## Parameters
    - conn: The database connection
    - root_oid: The OID of the partitioned (root) table
  """
  @spec check_partitioned_replica_identity(db_conn(), integer()) ::
          {:ok, :full | :default | :nothing | :index | :mixed} | {:error, Error.t()}
  def check_partitioned_replica_identity(conn, root_oid) do
    query = """
    with partitions as (
    select c.oid, c.relreplident
    from pg_inherits i
    join pg_class c on c.oid = i.inhrelid
    where i.inhparent = $1

    union all

    select $1 as oid, c.relreplident
    from pg_class c
    where c.oid = $1
    )
    select relreplident, count(*)
    from partitions
    group by relreplident
    """

    case query(conn, query, [root_oid]) do
      {:ok, %{rows: []}} ->
        {:error, Error.not_found(entity: :table, params: %{oid: root_oid})}

      {:ok, %{rows: rows}} ->
        if length(rows) > 1 do
          # Multiple different replica identity settings exist
          {:ok, :mixed}
        else
          # All partitions have the same replica identity
          [[relreplident, _count]] = rows

          identity =
            case relreplident do
              "f" -> :full
              "d" -> :default
              "n" -> :nothing
              "i" -> :index
            end

          {:ok, identity}
        end

      {:error, error} ->
        Logger.error("Failed to check replica identity for partitioned table #{root_oid}: #{inspect(error)}")
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
            {:error,
             Error.service(
               service: :postgres,
               message: "Failed to create replication slot",
               details: error
             )}
        end

      {:ok, _} ->
        :ok

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to check for existing replication slot",
           details: error
         )}
    end
  end

  def default_publication_init_sql(publication_name) do
    "create publication #{quote_name(publication_name)} for all tables with (publish_via_partition_root = true)"
  end

  @doc """
  Creates a logical replication publication if it doesn't already exist.

  ## Parameters
    - conn: The database connection
    - publication_name: The name of the publication to create
    - init_sql: Optional SQL to create the publication. If not provided, creates a publication for the public schema.

  ## Returns
    - :ok on success
    - {:error, Error.t()} on failure

  ## Examples
      # Create publication for all tables in the public schema
      create_publication(conn, "my_publication")

      # Create publication with custom SQL
      create_publication(conn, "my_publication", "create publication my_publication for tables in schema public")
  """
  @spec create_publication(db_conn(), name :: String.t(), init_sql :: String.t() | nil) :: :ok | {:error, Error.t()}
  def create_publication(conn, publication_name, init_sql \\ nil) do
    # Check if publication already exists
    check_query = "select 1 from pg_publication where pubname = $1"

    init_sql = init_sql || default_publication_init_sql(publication_name)

    with :ok <- verify_publication_init_sql(publication_name, init_sql),
         {:ok, %{num_rows: 0}} <- query(conn, check_query, [publication_name]),
         {:ok, _} <- query(conn, init_sql) do
      :ok
    else
      {:ok, _} ->
        :ok

      {:error, error} when is_error(error) ->
        {:error, error}

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to check or create publication",
           details: error
         )}
    end
  end

  defp verify_publication_init_sql(pub_name, init_sql) do
    regex = ~r/^\s*create\s+publication\s+"?#{Regex.escape(pub_name)}"?\s/

    if Regex.match?(regex, String.downcase(init_sql)) do
      :ok
    else
      {:error,
       Error.bad_request(
         message:
           "Invalid publication `init_sql`. Must be a `create publication` statement that matches supplied `name` of publication: #{init_sql}"
       )}
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

            is_struct(value, Pgvector) ->
              Pgvector.to_list(value)

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

            # Range type handling
            is_struct(value, Postgrex.Range) ->
              format_range(value)

            # This is the catch-all when encode is not implemented
            Jason.Encoder.impl_for(value) == Jason.Encoder.Any ->
              Logger.debug("[Postgres] No Jason.Encoder for #{inspect(value)}", column: col.name, table: table.name)
              nil

            true ->
              value
          end

        {col.name, casted_val}
      end)
    end)
  end

  # Helper function to format ranges consistently
  defp format_range(%Postgrex.Range{
         lower: lower,
         upper: upper,
         lower_inclusive: lower_inclusive,
         upper_inclusive: upper_inclusive
       }) do
    left_bracket = if lower_inclusive, do: "[", else: "("
    right_bracket = if upper_inclusive, do: "]", else: ")"
    "#{left_bracket}#{lower},#{upper}#{right_bracket}"
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
    "vector",
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
        Logger.error("[Postgres] Failed to check if table is in publication", error: error)

        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to check if table is in publication",
           details: error
         )}
    end
  end

  @doc """
  Gets publication information by name.
  Returns nil if publication doesn't exist.

  ## Example
    {:ok, %{
      "oid" => 4835544,
      "pubname" => "sequin_pub",
      "pubinsert" => true,
      "pubupdate" => true,
      "pubdelete" => true,
      "pubtruncate" => true,
      "pubviaroot" => false
    }}
  """
  @spec get_publication(db_conn(), String.t()) ::
          {:ok, map()} | {:error, Error.t()}
  def get_publication(conn, publication_name) do
    query = """
    select *
    from pg_publication
    where pubname = $1
    """

    case query(conn, query, [publication_name]) do
      {:ok, %{rows: []}} ->
        {:error, Error.not_found(entity: :publication, params: %{name: publication_name})}

      {:ok, res} ->
        {:ok, res |> result_to_maps() |> List.first()}

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to get publication information",
           details: error
         )}
    end
  end

  @type table_with_replica_identity :: %{
          table_schema: String.t(),
          table_name: String.t(),
          table_oid: integer(),
          relation_kind: String.t(),
          replica_identity: String.t() | nil,
          partition_replica_identities: list(%{replica_identity: String.t(), count: integer()}) | nil
        }
  @spec all_tables_with_replica_identities_in_publication(db_conn(), String.t()) ::
          {:ok, list(table_with_replica_identity())} | {:error, Error.t()}
  def all_tables_with_replica_identities_in_publication(conn, publication_name) do
    query = """
    SELECT
        pt.schemaname as table_schema,
        pt.tablename as table_name,
        c.oid AS table_oid,
        c.relkind AS relation_kind,
        CASE
            WHEN c.relkind = 'p' THEN NULL  -- Will be handled by partition rollup
            ELSE c.relreplident
        END AS replica_identity,
        CASE
            WHEN c.relkind = 'p' THEN partition_identities.identity_counts
            ELSE NULL
        END AS partition_replica_identities
    FROM pg_publication p
    JOIN pg_publication_tables pt ON p.pubname = pt.pubname
    JOIN pg_class c ON c.relname = pt.tablename
    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = pt.schemaname
    LEFT JOIN LATERAL (
        WITH partitions AS (
            SELECT child.oid, child.relreplident
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            WHERE i.inhparent = c.oid
            UNION ALL
            SELECT c.oid, c.relreplident
        )
        SELECT json_agg(
            json_build_object(
                'replica_identity', replica_identity,
                'count', count
            )
        ) AS identity_counts
        FROM (
            SELECT relreplident as replica_identity, count(*)
            FROM partitions
            GROUP BY relreplident
        ) counts
    ) partition_identities ON c.relkind = 'p'
    WHERE p.pubname = $1
    ORDER BY pt.schemaname, pt.tablename;
    """

    case query(conn, query, [publication_name]) do
      {:ok, %Postgrex.Result{} = result} -> {:ok, result_to_maps(result)}
      {:error, error} -> {:error, error}
    end
  end

  def any_partitioned_tables?(tables_with_replica_identities) do
    Enum.any?(tables_with_replica_identities, fn table ->
      table["relation_kind"] == "p"
    end)
  end

  def any_tables_without_full_replica_identity?(tables_with_replica_identities) do
    Enum.any?(tables_with_replica_identities, fn table ->
      table["relation_kind"] == "r" and table["replica_identity"] != "f"
    end)
  end

  def any_partitioned_tables_without_full_replica_identity?(tables_with_replica_identities) do
    Enum.any?(tables_with_replica_identities, fn table ->
      table["relation_kind"] == "p" and Enum.any?(table["partition_replica_identities"], &(&1["replica_identity"] != "f"))
    end)
  end

  @doc """
  Gets the relation kind ('r' for regular table, 'p' for partitioned table, etc) for a given table OID.
  See: https://www.postgresql.org/docs/current/catalog-pg-class.html#CATALOG-PG-CLASS-TABLE
  """
  @spec get_relation_kind(db_conn(), integer()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def get_relation_kind(conn, table_oid) do
    query = """
    select relkind
    from pg_class
    where oid = $1
    """

    case query(conn, query, [table_oid]) do
      {:ok, %{rows: [[relkind]]}} ->
        {:ok, relkind}

      {:ok, %{rows: []}} ->
        {:error, Error.not_found(entity: :table, params: %{oid: table_oid})}

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to get relation kind",
           details: error
         )}
    end
  end

  @doc """
  Gets the replication lag in bytes for the database's replication slot.
  Returns {:ok, bytes} on success, where bytes is a Decimal,
  or {:error, error} on failure.
  """
  @spec replication_lag_bytes(db_conn(), String.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def replication_lag_bytes(conn, slot_name) do
    query = """
    select pg_wal_lsn_diff(current.lsn, confirmed_flush_lsn) as replication_lag_bytes
    from pg_replication_slots, (select case when pg_is_in_recovery() then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end lsn) current
    where slot_name = $1;
    """

    case query(conn, query, [slot_name]) do
      {:ok, %{rows: [[bytes]]}} -> {:ok, Decimal.to_integer(bytes)}
      {:ok, %{rows: []}} -> {:error, Error.not_found(entity: :replication_slot, params: %{slot_name: slot_name})}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Gets both restart_lsn and confirmed_flush_lsn for a replication slot.
  """
  @spec get_replication_lsns(db_conn(), String.t()) ::
          {:ok, %{restart_lsn: non_neg_integer() | nil, confirmed_flush_lsn: non_neg_integer() | nil}}
          | {:error, Error.t()}
  def get_replication_lsns(conn, slot_name) do
    query = """
    SELECT restart_lsn, confirmed_flush_lsn
    FROM pg_replication_slots
    WHERE slot_name = $1
    """

    case query(conn, query, [slot_name]) do
      {:ok, %{rows: [[restart_lsn, confirmed_flush_lsn]]}} ->
        restart_lsn = unless is_nil(restart_lsn), do: lsn_to_int(restart_lsn)
        confirmed_flush_lsn = unless is_nil(confirmed_flush_lsn), do: lsn_to_int(confirmed_flush_lsn)
        {:ok, %{restart_lsn: restart_lsn, confirmed_flush_lsn: confirmed_flush_lsn}}

      {:ok, %{rows: []}} ->
        {:error, Error.not_found(entity: :replication_slot, params: %{name: slot_name})}

      {:error, _} = error ->
        error
    end
  end

  @logical_message_table_name Constants.logical_messages_table_name()

  def logical_messages_table_ddl do
    """
    CREATE TABLE IF NOT EXISTS public.#{@logical_message_table_name} (
      id SERIAL PRIMARY KEY,
      slot_id TEXT NOT NULL,
      subject TEXT NOT NULL,
      content JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(slot_id, subject)
    );
    """
  end

  @doc """
  Creates the logical_messages table used for heartbeats in Postgres < 14.
  This table should have a primary key on (slot_id, subject) for efficient upserts.
  """
  @spec create_logical_messages_table(database :: PostgresDatabase.t()) :: :ok | {:error, Error.t()}
  def create_logical_messages_table(%PostgresDatabase{} = database) do
    case query(database, logical_messages_table_ddl()) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        {:error,
         Error.service(
           service: :postgres,
           message: "Failed to create pubblic.#{@logical_message_table_name} table",
           details: error
         )}
    end
  end

  @doc """
  Checks if the #{@logical_message_table_name} table exists.
  """
  @spec has_sequin_logical_messages_table?(PostgresDatabase.t()) :: boolean()
  def has_sequin_logical_messages_table?(%PostgresDatabase{} = database) do
    query = """
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_name = '#{@logical_message_table_name}'
      AND table_schema = 'public'
    )
    """

    case query(database, query) do
      {:ok, %{rows: [[true]]}} -> true
      {:ok, %{rows: [[false]]}} -> false
    end
  end

  @spec sequin_logical_messages_table_in_publication?(PostgresDatabase.t()) :: boolean()
  def sequin_logical_messages_table_in_publication?(%PostgresDatabase{} = database) do
    pub_name = database.replication_slot.publication_name

    query = """
    SELECT EXISTS (
      SELECT 1
      FROM pg_publication_tables
      WHERE pubname = '#{pub_name}'
      AND tablename = '#{@logical_message_table_name}'
      AND schemaname = 'public'
    )
    """

    case query(database, query) do
      {:ok, %{rows: [[true]]}} -> true
      {:ok, %{rows: [[false]]}} -> false
    end
  end

  @spec upsert_logical_message(db_conn(), slot_id :: String.t(), subject :: String.t(), content :: String.t()) ::
          {:ok, appx_lsn :: integer()} | {:error, Error.t()}
  def upsert_logical_message(database_or_conn, slot_id, subject, content) do
    upsert_query = """
    INSERT INTO public.#{@logical_message_table_name} (slot_id, subject, content)
    VALUES ($1, $2, $3)
    ON CONFLICT (slot_id, subject) DO UPDATE SET content = $3, updated_at = NOW()
    """

    with {:ok, _} <- query(database_or_conn, upsert_query, [slot_id, subject, content]),
         {:ok, %{rows: [[appx_lsn]]}} <- query(database_or_conn, "SELECT pg_current_wal_lsn()") do
      {:ok, appx_lsn}
    end
  end
end
