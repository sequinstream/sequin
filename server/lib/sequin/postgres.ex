defmodule Sequin.Postgres do
  @moduledoc false
  alias Sequin.Repo

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

  def quote_names(names) do
    Enum.map_intersperse(names, ?,, &quote_name/1)
  end

  @doc """
  quote_name is vendored from Ecto.Adapters.Postgres.Connection.
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
