defmodule Sequin.Consumers.MysqlSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:host, :port, :database, :table_name, :username, :ssl]}
  @derive {Inspect, except: [:password]}

  @primary_key false
  typed_embedded_schema do
    field(:type, Ecto.Enum, values: [:mysql], default: :mysql)
    field(:host, :string)
    field(:port, :integer, default: 3306)
    field(:database, :string)
    field(:table_name, :string)
    field(:username, :string)
    field(:password, Encrypted.Field)
    field(:ssl, :boolean, default: false)
    field(:batch_size, :integer, default: 100)
    field(:timeout_seconds, :integer, default: 30)
    field(:upsert_on_duplicate, :boolean, default: true)
    field(:routing_mode, Ecto.Enum, values: [:dynamic, :static], default: :static)
    field(:connection_id, :string, virtual: true)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :host,
      :port,
      :database,
      :table_name,
      :username,
      :password,
      :ssl,
      :batch_size,
      :timeout_seconds,
      :upsert_on_duplicate,
      :routing_mode
    ])
    |> validate_required([:host, :database, :username, :password])
    |> validate_routing()
    |> validate_number(:port, greater_than: 0, less_than_or_equal_to: 65_535)
    |> validate_number(:batch_size, greater_than: 0, less_than_or_equal_to: 10_000)
    |> validate_number(:timeout_seconds, greater_than: 0, less_than_or_equal_to: 300)
    |> validate_length(:host, max: 255)
    |> validate_length(:database, max: 64)
    |> validate_length(:table_name, max: 64)
    |> validate_length(:username, max: 32)
    |> validate_table_name()
  end

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :table_name, nil)

      routing_mode == :static ->
        validate_required(changeset, [:table_name])

      true ->
        add_error(changeset, :routing_mode, "is required")
    end
  end

  defp validate_table_name(changeset) do
    validate_format(changeset, :table_name, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/,
      message: "must be a valid MySQL table name (alphanumeric and underscores, starting with letter or underscore)"
    )
  end

  @doc """
  Generate a unique connection ID for caching purposes.
  """
  def connection_id(%__MODULE__{} = sink) do
    "#{sink.host}:#{sink.port}/#{sink.database}@#{sink.username}"
  end

  def connection_opts(%__MODULE__{} = sink) do
    opts = [
      hostname: sink.host,
      port: sink.port,
      database: sink.database,
      username: sink.username,
      password: sink.password,
      timeout: to_timeout(second: sink.timeout_seconds),
      pool_size: 10
    ]

    if sink.ssl do
      Keyword.put(opts, :ssl, true)
    else
      opts
    end
  end
end
