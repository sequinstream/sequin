defmodule Sequin.Postgres do
  @moduledoc false
  alias Sequin.Repo

  def list_schemas(conn) do
    with {:ok, %{rows: rows}} <- Postgrex.query(conn, "SELECT schema_name FROM information_schema.schemata", []) do
      filtered_schemas =
        rows
        |> List.flatten()
        |> Enum.reject(&(&1 in ["pg_toast", "pg_catalog", "information_schema"]))

      {:ok, filtered_schemas}
    end
  end

  def list_tables(conn, schema) do
    with {:ok, %{rows: rows}} <-
           Postgrex.query(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", [schema]) do
      {:ok, List.flatten(rows)}
    end
  end

  def fetch_table_oid(conn, schema, table) do
    case Postgrex.query(conn, "SELECT '#{schema}.#{table}'::regclass::oid", []) do
      {:ok, %{rows: [[oid]]}} -> oid
      _ -> nil
    end
  end

  def list_columns(conn, schema, table) do
    res = Postgrex.query(conn, "
    SELECT attnum, attname, format_type(atttypid, atttypmod)
    FROM pg_attribute
    WHERE attrelid = '#{schema}.#{table}'::regclass AND attnum > 0 AND NOT attisdropped
    ORDER BY attnum
  ", [])

    with {:ok, %{rows: rows}} <- res do
      {:ok, rows}
    end
  end

  def try_advisory_xact_lock(term) do
    lock_key = :erlang.phash2(term)

    case Repo.query("SELECT pg_try_advisory_xact_lock($1)", [lock_key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :locked}
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

  def identifier(identifier, opts \\ []) when is_list(opts) do
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

    [?", name, ?"]
  end
end
