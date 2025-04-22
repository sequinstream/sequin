defmodule Sequin.Postgres.ValueCaster do
  @moduledoc false
  alias Sequin.Error
  alias Sequin.Postgres.ArrayLexer

  require Logger

  @spec cast(type :: String.t(), value :: any()) :: {:ok, any()} | {:error, Error.t()}
  def cast("bool", "t"), do: {:ok, true}
  def cast("bool", "f"), do: {:ok, false}

  def cast("_" <> _type, "{}"), do: {:ok, []}

  def cast("_" <> type, array_string) when is_binary(array_string) do
    with {:ok, uncasted_list} <- ArrayLexer.lex(array_string) do
      cast(type, uncasted_list)
    end
  end

  def cast(type, value) when type in ["json", "jsonb"] and is_binary(value) do
    case Jason.decode(value) do
      {:ok, json} ->
        {:ok, json}

      {:error, %Jason.DecodeError{} = error} ->
        Logger.error("Failed to decode JSON value: #{inspect(error, limit: 10_000)}")

        wrapped_error =
          Error.service(
            service: :postgres_replication_slot,
            message: "Failed to decode JSON value: #{inspect(error)}",
            details: %{error: Exception.message(error)},
            code: :invalid_json
          )

        {:error, wrapped_error}
    end
  end

  def cast("vector", nil), do: {:ok, nil}

  def cast("vector", value_str) when is_binary(value_str) do
    list =
      value_str
      |> String.trim("[")
      |> String.trim("]")
      |> String.split(",")
      |> Enum.map(fn num ->
        {float, ""} = Float.parse(num)
        float
      end)

    {:ok, list}
  end

  def cast(type, value) when is_list(value) do
    res =
      Enum.reduce_while(value, {:ok, []}, fn elem, {:ok, acc} ->
        case cast(type, elem) do
          {:ok, casted_elem} -> {:cont, {:ok, [casted_elem | acc]}}
          error -> {:halt, error}
        end
      end)

    case res do
      {:ok, res} -> {:ok, Enum.reverse(res)}
      error -> error
    end
  end

  def cast(type, value) do
    case Ecto.Type.cast(string_to_ecto_type(type), value) do
      {:ok, casted_value} -> {:ok, casted_value}
      # Fallback to original value if casting fails
      :error -> {:ok, value}
    end
  end

  @postgres_to_ecto_type_mapping %{
    # Numeric Types
    "int2" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "float4" => :float,
    "float8" => :float,
    "numeric" => :decimal,
    "money" => :decimal,
    # Character Types
    "char" => :string,
    "varchar" => :string,
    "text" => :string,
    # Binary Data Types
    "bytea" => :binary,
    # Date/Time Types
    "timestamp" => :naive_datetime,
    "timestamptz" => :utc_datetime,
    "date" => :date,
    "time" => :time,
    "timetz" => :time,
    # Ecto doesn't have a direct interval type
    "interval" => :map,
    # Boolean Type
    "bool" => :boolean,
    # Geometric Types
    "point" => {:array, :float},
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,
    # Network Address Types
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,
    # Bit String Types
    "bit" => :string,
    "bit_varying" => :string,
    # Text Search Types
    "tsvector" => :string,
    "tsquery" => :string,
    # UUID Type
    "uuid" => Ecto.UUID,
    # XML Type
    "xml" => :string,
    # JSON Types
    "json" => :map,
    "jsonb" => :map,
    # Arrays
    "_text" => {:array, :string},
    # Composite Types
    "composite" => :map,
    # Range Types
    "range" => {:array, :any},
    # Domain Types
    "domain" => :any,
    # Object Identifier Types
    "oid" => :integer,
    # pg_lsn Type
    "pg_lsn" => :string,
    # Pseudotypes
    "any" => :any
  }

  defp string_to_ecto_type(type) do
    Map.get(@postgres_to_ecto_type_mapping, type, :string)
  end
end
