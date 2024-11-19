defmodule Sequin.Consumers.RedisSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias __MODULE__

  @derive {Jason.Encoder, only: [:host, :port, :stream_key]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:redis], default: :redis
    field :host, :string
    field :port, :integer
    field :username, :string
    field :password, Sequin.Encrypted.Binary
    field :tls, :boolean, default: false
    field :stream_key, :string
    field :database, :integer, default: 0
    field :connection_id, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:host, :port, :username, :password, :tls, :stream_key, :database])
    |> validate_required([:host, :port, :stream_key])
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
    |> validate_number(:database, greater_than_or_equal_to: 0)
    |> validate_length(:stream_key, max: 255)
    |> validate_redis_host()
    |> put_new_connection_id()
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

  defp put_new_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end

  def redis_url(sink, opts \\ []) do
    obscure_password = Keyword.get(opts, :obscure_password, true)

    auth = build_auth_string(sink, obscure_password)
    "#{protocol(sink)}#{auth}#{sink.host}:#{sink.port}/#{sink.database}"
  end

  defp build_auth_string(%RedisSink{username: nil, password: nil}, _obscure), do: ""

  defp build_auth_string(%RedisSink{username: nil, password: password}, obscure) do
    "#{format_password(password, obscure)}@"
  end

  defp build_auth_string(%RedisSink{username: username, password: nil}, _obscure) do
    "#{username}@"
  end

  defp build_auth_string(%RedisSink{username: username, password: password}, obscure) do
    "#{username}:#{format_password(password, obscure)}@"
  end

  defp format_password(_, true), do: "******"
  defp format_password(password, false), do: password

  defp protocol(%RedisSink{tls: true}), do: "rediss://"
  defp protocol(%RedisSink{tls: false}), do: "redis://"

  defp prod_env?, do: Application.get_env(:sequin, :env) == :prod

  defp self_hosted?, do: Application.get_env(:sequin, :self_hosted)
end
