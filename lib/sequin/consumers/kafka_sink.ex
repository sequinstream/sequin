defmodule Sequin.Consumers.KafkaSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias __MODULE__
  alias Sequin.Encrypted.Field, as: EncryptedField
  alias Sequin.Sinks.Kafka.AwsMskIam

  @derive {Jason.Encoder, only: [:hosts, :topic]}
  @derive {Inspect, except: [:password, :aws_secret_access_key]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:kafka], default: :kafka
    field :hosts, :string
    field :username, :string
    field :password, EncryptedField
    field :tls, :boolean, default: false
    field :topic, :string
    field :sasl_mechanism, Ecto.Enum, values: [:plain, :scram_sha_256, :scram_sha_512, :aws_msk_iam]
    field :aws_region, :string
    field :aws_access_key_id, :string
    field :aws_secret_access_key, EncryptedField
    field :connection_id, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :hosts,
      :username,
      :password,
      :tls,
      :topic,
      :sasl_mechanism,
      :aws_region,
      :aws_access_key_id,
      :aws_secret_access_key
    ])
    |> validate_required([:hosts, :topic, :tls])
    |> validate_length(:topic, max: 255)
    |> validate_hosts()
    |> validate_sasl_credentials()
    |> put_new_connection_id()
  end

  defp validate_hosts(changeset) do
    hosts = get_field(changeset, :hosts)

    if hosts do
      hosts_valid? =
        hosts
        |> String.split(",")
        |> Enum.all?(fn host ->
          case String.split(host, ":") do
            [_host, port] ->
              case Integer.parse(port) do
                {port_num, ""} -> port_num > 0 and port_num < 65_536
                _ -> false
              end

            _ ->
              false
          end
        end)

      if hosts_valid? do
        changeset
      else
        add_error(changeset, :hosts, "must be a comma-separated list of host:port pairs with valid ports (1-65535)")
      end
    else
      changeset
    end
  end

  defp validate_sasl_credentials(changeset) do
    sasl_mechanism = get_field(changeset, :sasl_mechanism)
    username = get_field(changeset, :username)
    password = get_field(changeset, :password)

    cond do
      sasl_mechanism in [:plain, :scram_sha_256, :scram_sha_512] ->
        validate_required(changeset, [:username, :password],
          message: "is required when SASL Mechanism is #{sasl_mechanism}"
        )

      sasl_mechanism == :aws_msk_iam ->
        changeset =
          validate_required(changeset, [:aws_access_key_id, :aws_secret_access_key, :aws_region],
            message: "is required when SASL Mechanism is #{sasl_mechanism}"
          )

        if get_field(changeset, :tls) do
          changeset
        else
          add_error(changeset, :tls, "is required when SASL Mechanism is #{sasl_mechanism}")
        end

      username || password ->
        add_error(changeset, :sasl_mechanism, "must be set when SASL credentials are provided")

      true ->
        changeset
    end
  end

  defp put_new_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end

  def kafka_url(sink, opts \\ []) do
    obscure_password = Keyword.get(opts, :obscure_password, true)

    auth = build_auth_string(sink, obscure_password)
    "#{protocol(sink)}#{auth}#{sink.hosts}"
  end

  defp build_auth_string(%KafkaSink{username: nil, password: nil}, _obscure), do: ""

  defp build_auth_string(%KafkaSink{username: nil, password: password}, obscure) do
    "#{format_password(password, obscure)}@"
  end

  defp build_auth_string(%KafkaSink{username: username, password: nil}, _obscure) do
    "#{username}@"
  end

  defp build_auth_string(%KafkaSink{username: username, password: password}, obscure) do
    "#{username}:#{format_password(password, obscure)}@"
  end

  defp format_password(_, true), do: "******"
  defp format_password(password, false), do: password

  defp protocol(%KafkaSink{tls: true}), do: "kafka+ssl://"
  defp protocol(%KafkaSink{tls: false}), do: "kafka://"

  def hosts(%KafkaSink{hosts: hosts}) do
    hosts
    |> String.split(",")
    |> Enum.map(fn host ->
      [host, port] = String.split(host, ":")
      {String.trim(host), String.to_integer(port)}
    end)
  end

  @doc """
  Converts a KafkaSink into configuration options for :brod.
  """
  def to_brod_config(%__MODULE__{} = sink) do
    []
    |> maybe_add_sasl(sink)
    |> maybe_add_ssl(sink)
    |> Keyword.put(:query_api_versions, true)
    |> Keyword.put(:auto_start_producers, true)
  end

  # Add SASL authentication if username/password are configured
  defp maybe_add_sasl(config, %{sasl_mechanism: :aws_msk_iam} = sink) do
    Keyword.put(
      config,
      :sasl,
      {:callback, AwsMskIam.Auth, {:AWS_MSK_IAM, sink.aws_access_key_id, sink.aws_secret_access_key, sink.aws_region}}
    )
  end

  defp maybe_add_sasl(config, %{sasl_mechanism: mechanism} = sink) when not is_nil(mechanism) do
    Keyword.put(config, :sasl, {mechanism, sink.username, sink.password})
  end

  defp maybe_add_sasl(config, _), do: config

  # Add SSL configuration if TLS is enabled
  defp maybe_add_ssl(config, %{tls: true}) do
    Keyword.put(config, :ssl, true)
  end

  defp maybe_add_ssl(config, _), do: config
end
