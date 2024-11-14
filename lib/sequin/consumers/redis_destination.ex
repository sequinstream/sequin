defmodule Sequin.Consumers.RedisDestination do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias __MODULE__

  @derive {Jason.Encoder, only: [:host, :port, :stream_key]}
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:redis], default: :redis
    field :host, :string
    field :port, :integer
    field :username, :string
    field :password, Sequin.Encrypted.Binary
    field :tls, :boolean, default: false
    field :stream_key, :string
    field :database, :integer, default: 0
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:host, :port, :username, :password, :tls, :stream_key, :database])
    |> validate_required([:host, :port, :stream_key])
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
    |> validate_number(:database, greater_than_or_equal_to: 0)
    |> validate_length(:stream_key, max: 255)
  end

  def redis_url(%RedisDestination{host: host, port: port, database: database} = destination) do
    "#{protocol(destination)}#{host}:#{port}/#{database}"
  end

  def redis_url(%RedisDestination{host: host, port: port, password: password, database: database} = destination) do
    "#{protocol(destination)}#{password}@#{host}:#{port}/#{database}"
  end

  def redis_url(
        %RedisDestination{host: host, port: port, username: username, password: password, database: database} =
          destination
      ) do
    "#{protocol(destination)}#{username}:#{password}@#{host}:#{port}/#{database}"
  end

  defp protocol(%RedisDestination{tls: true}), do: "rediss://"
  defp protocol(%RedisDestination{tls: false}), do: "redis://"
end
