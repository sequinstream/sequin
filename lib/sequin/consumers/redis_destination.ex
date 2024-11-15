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
    |> validate_redis_host()
  end

  defp validate_redis_host(changeset) do
    if prod_env?() and not self_hosted?() do
      case System.fetch_env("REDIS_URL") do
        {:ok, redis_url} ->
          %URI{host: system_host} = URI.parse(redis_url)
          host = get_field(changeset, :host)

          if host == system_host do
            add_error(changeset, :host, "is invalid")
          else
            changeset
          end

        :error ->
          changeset
      end
    else
      changeset
    end
  end

  def redis_url(destination, opts \\ []) do
    obscure_password = Keyword.get(opts, :obscure_password, true)

    auth = build_auth_string(destination, obscure_password)
    "#{protocol(destination)}#{auth}#{destination.host}:#{destination.port}/#{destination.database}"
  end

  defp build_auth_string(%RedisDestination{username: nil, password: nil}, _obscure), do: ""

  defp build_auth_string(%RedisDestination{username: nil, password: password}, obscure) do
    "#{format_password(password, obscure)}@"
  end

  defp build_auth_string(%RedisDestination{username: username, password: nil}, _obscure) do
    "#{username}@"
  end

  defp build_auth_string(%RedisDestination{username: username, password: password}, obscure) do
    "#{username}:#{format_password(password, obscure)}@"
  end

  defp format_password(_, true), do: "******"
  defp format_password(password, false), do: password

  defp protocol(%RedisDestination{tls: true}), do: "rediss://"
  defp protocol(%RedisDestination{tls: false}), do: "redis://"

  defp prod_env?, do: Application.get_env(:sequin, :env) == :prod

  defp self_hosted?, do: Application.get_env(:sequin, :self_hosted)
end
