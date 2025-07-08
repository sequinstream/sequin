defmodule Sequin.Consumers.RedisStreamSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias __MODULE__

  @derive {Jason.Encoder, only: [:host, :port, :stream_key]}
  @derive {Inspect, except: [:password]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:redis_stream], default: :redis_stream
    field :host, :string
    field :port, :integer
    field :username, :string
    field :password, Sequin.Encrypted.Binary
    field :tls, :boolean, default: false
    field :stream_key, :string
    field :database, :integer, default: 0
    field :connection_id, :string
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:host, :port, :username, :password, :tls, :stream_key, :database, :routing_mode])
    |> validate_required([:host, :port])
    |> validate_routing()
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
    |> validate_number(:database, greater_than_or_equal_to: 0)
    |> validate_length(:stream_key, max: 255)
    |> validate_redis_host()
    |> put_new_connection_id()
  end

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :stream_key, nil)

      routing_mode == :static ->
        validate_required(changeset, [:stream_key])

      true ->
        add_error(changeset, :routing_mode, "is required")
    end
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

  def start_opts(%RedisStreamSink{} = sink) do
    # https://hexdocs.pm/eredis/eredis.html#start_link-1
    [
      host: to_charlist(sink.host),
      port: sink.port,
      reconnect_sleep: to_timeout(second: 30),
      database: sink.database
    ]
    |> maybe_put_tls(sink)
    |> maybe_put_username(sink)
    |> maybe_put_password(sink)
  end

  defp maybe_put_tls(opts, %RedisStreamSink{tls: true}), do: Keyword.put(opts, :tls, verify: :verify_none)
  defp maybe_put_tls(opts, _), do: opts

  defp maybe_put_username(opts, %RedisStreamSink{username: nil}), do: opts
  defp maybe_put_username(opts, %RedisStreamSink{username: username}), do: Keyword.put(opts, :username, username)

  defp maybe_put_password(opts, %RedisStreamSink{password: nil}), do: opts
  defp maybe_put_password(opts, %RedisStreamSink{password: password}), do: Keyword.put(opts, :password, password)

  defp build_auth_string(%RedisStreamSink{username: nil, password: nil}, _obscure), do: ""

  defp build_auth_string(%RedisStreamSink{username: nil, password: password}, obscure) do
    "#{format_password(password, obscure)}@"
  end

  defp build_auth_string(%RedisStreamSink{username: username, password: nil}, _obscure) do
    "#{username}@"
  end

  defp build_auth_string(%RedisStreamSink{username: username, password: password}, obscure) do
    "#{username}:#{format_password(password, obscure)}@"
  end

  defp format_password(_, true), do: "******"
  defp format_password(password, false), do: password

  defp protocol(%RedisStreamSink{tls: true}), do: "rediss://"
  defp protocol(%RedisStreamSink{tls: false}), do: "redis://"

  defp prod_env?, do: Application.get_env(:sequin, :env) == :prod

  defp self_hosted?, do: Application.get_env(:sequin, :self_hosted)
end
