defmodule Sequin.Postgres do
  @moduledoc false
  import Ecto.Query, only: [from: 2]

  alias Sequin.Consumers.SourceTable
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.ValidationError
  alias Sequin.Repo

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

  def query(mod, query, params, opts) when is_atom(mod) do
    mod.query(query, params, opts)
  end

  def query(%PostgresDatabase{} = db, query, params, opts) do
    case ConnectionCache.connection(db) do
      {:ok, conn_or_mod} ->
        query(conn_or_mod, query, params, opts)

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
      _ -> :string
    end
  end

  def result_to_maps(%Postgrex.Result{} = result) do
    %{columns: columns, rows: rows} = result

    Enum.map(rows, fn row ->
      columns |> Enum.zip(row) |> Map.new()
    end)
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
    SELECT DISTINCT ON (n.nspname, c.relname, a.attnum)
      n.nspname AS schema,
      c.relname AS table_name,
      c.oid AS table_oid,
      a.attnum,
      a.attname AS column_name,
      pg_catalog.format_type(a.atttypid, -1) AS column_type,
      COALESCE(i.indisprimary, false) AS is_pk
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_attribute a ON c.oid = a.attrelid
    LEFT JOIN pg_index i ON c.oid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE n.nspname IN (#{schemas_list})
      AND c.relkind = 'r'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY n.nspname, c.relname, a.attnum, i.indisprimary DESC NULLS LAST
    """

    case query(conn, query) do
      {:ok, %{rows: rows}} ->
        tables = process_table_rows(rows)
        {:ok, tables}

      {:error, _} = error ->
        error
    end
  end

  defp process_table_rows(rows) do
    rows
    |> Enum.group_by(fn [schema, table_name, table_oid | _] -> {schema, table_name, table_oid} end)
    |> Enum.map(fn {{schema, table_name, table_oid}, columns} ->
      %PostgresDatabase.Table{
        oid: table_oid,
        schema: schema,
        name: table_name,
        columns:
          Enum.map(columns, fn [_, _, _, attnum, column_name, column_type, is_pk] ->
            %PostgresDatabase.Table.Column{
              attnum: attnum,
              name: column_name,
              type: column_type,
              is_pk?: is_pk
            }
          end)
      }
    end)
  end

  def check_replication_slot_exists(conn, slot_name) do
    query = "select 1 from pg_replication_slots where slot_name = $1"

    case query(conn, query, [slot_name]) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, Error.validation(summary: "Replication slot '#{slot_name}' does not exist")}

      {:error, %Postgrex.Error{} = error} ->
        {:error, ValidationError.from_postgrex("Failed to check replication slot: ", error)}
    end
  rescue
    error in [DBConnection.ConnectionError] ->
      {:error, Error.validation(summary: "Failed to check replication slot: #{error.message}")}
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
    query =
      "select pg_is_in_recovery(), current_setting('max_replication_slots')::int > 0"

    case query(conn, query) do
      {:ok, %{rows: [[is_in_recovery, has_replication_slots]]}} ->
        cond do
          is_in_recovery ->
            {:error, Error.validation(summary: "Database is in recovery mode and cannot be used for replication")}

          not has_replication_slots ->
            {:error, Error.validation(summary: "Database does not have replication slots enabled")}

          true ->
            :ok
        end

      {:error, %Postgrex.Error{} = error} ->
        {:error, ValidationError.from_postgrex("Failed to check replication permissions: ", error)}
    end
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
    case query(conn, "SELECT '#{schema}.#{table}'::regclass::oid") do
      {:ok, %{rows: [[oid]]}} -> oid
      _ -> nil
    end
  end

  def list_columns(conn, schema, table) do
    res =
      query(
        conn,
        """
        select distinct on (a.attnum)
        a.attnum,
        a.attname,
        pg_catalog.format_type(a.atttypid, -1) as simple_type,
        coalesce(i.indisprimary, false) as is_pk
        from pg_attribute a
        join pg_class c on a.attrelid = c.oid
        join pg_namespace n on c.relnamespace = n.oid
        left join pg_index i on c.oid = i.indrelid and a.attnum = any(i.indkey)
        where n.nspname = $1
        and c.relname = $2
        and a.attnum > 0
        and not a.attisdropped
        order by a.attnum
        """,
        [schema, table]
      )

    case res do
      {:ok, %{rows: []}} -> {:error, Error.not_found(entity: "table", params: %{schema: schema, table: table})}
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, _} = error -> error
    end
  end

  def try_advisory_xact_lock(term) do
    lock_key = :erlang.phash2(term)

    case Repo.query("SELECT pg_try_advisory_xact_lock($1)", [lock_key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :locked}
    end
  end

  def check_replica_identity(conn, schema, table) when is_binary(schema) and is_binary(table) do
    query = """
    SELECT relreplident
    FROM pg_class
    WHERE oid = '#{schema}.#{table}'::regclass;
    """

    case query(conn, query) do
      {:ok, %{rows: [["f"]]}} -> {:ok, :full}
      {:ok, %{rows: [["d"]]}} -> {:ok, :default}
      {:ok, %{rows: [["n"]]}} -> {:ok, :nothing}
      {:ok, %{rows: [["i"]]}} -> {:ok, :index}
      {:error, _} = error -> error
    end
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

  def quote_name(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

  def quote_name(name) when is_atom(name) do
    quote_name(Atom.to_string(name))
  end

  def quote_name(name) when is_binary(name) do
    if String.contains?(name, "\"") do
      {:error, "bad literal/field/index/table name #{inspect(name)} (\" is not permitted)"}
    end

    to_string([?", name, ?"])
  end
end
