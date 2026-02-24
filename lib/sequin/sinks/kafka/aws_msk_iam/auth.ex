defmodule Sequin.Sinks.Kafka.AwsMskIam.Auth do
  @moduledoc """
  SASL AWS_MSK_IAM auth backend implementation for brod Erlang library.
  To authenticate, supply aws_secret_key_id and aws_secret_access_key with access to MSK cluster
  """
  @behaviour :kpro_auth_backend

  alias Sequin.Sinks.Kafka.AwsMskIam.SignedPayloadGenerator

  require Logger

  @handshake_version 1

  # Handle task role credentials
  def auth(
        host,
        sock,
        mod,
        client_id,
        timeout,
        {:AWS_MSK_IAM = mechanism, :task_role, aws_region} = _sasl_opts
      ) do
    case :aws_credentials.get_credentials() do
      :undefined ->
        {:error,
         "Task role credentials not found. Ensure AWS credentials are available via environment variables, credentials file, ECS task role, web identity token, or EC2 metadata."}

      credentials when is_map(credentials) ->
        aws_secret_key_id = Map.get(credentials, :access_key_id)
        aws_secret_access_key = Map.get(credentials, :secret_access_key)

        auth(
          host,
          sock,
          mod,
          client_id,
          timeout,
          {mechanism, aws_secret_key_id, aws_secret_access_key, aws_region}
        )

      other ->
        {:error, "Unexpected credential format: #{inspect(other)}"}
    end
  end

  def auth(
        _host,
        _sock,
        _mod,
        _client_id,
        _timeout,
        {:AWS_MSK_IAM = _mechanism, nil = _aws_secret_key_id, _aws_secret_access_key, _aws_region} = _sasl_opts
      ) do
    {:error, "AWS Secret Key ID is empty"}
  end

  def auth(
        _host,
        _sock,
        _mod,
        _client_id,
        _timeout,
        {:AWS_MSK_IAM = _mechanism, _aws_secret_key_id, nil = _aws_secret_access_key, _aws_region} = _sasl_opts
      ) do
    {:error, "AWS Secret Access Key is empty"}
  end

  def auth(
        _host,
        _sock,
        _mod,
        _client_id,
        _timeout,
        {:AWS_MSK_IAM = _mechanism, _aws_secret_key_id, _aws_secret_access_key, nil = _aws_region} = _sasl_opts
      ) do
    {:error, "AWS Region is empty"}
  end

  # The following code is based on the implmentation of SASL handshake implementation from kafka_protocol Erlang library
  # Ref: https://github.com/kafka4beam/kafka_protocol/blob/master/src/kpro_sasl.erl
  @impl true
  @spec auth(
          any(),
          port(),
          :gen_tcp | :ssl,
          binary(),
          :infinity | non_neg_integer(),
          {:AWS_MSK_IAM, binary(), binary(), binary()}
        ) ::
          :ok | {:error, any()}
  def auth(
        host,
        sock,
        mod,
        client_id,
        timeout,
        {:AWS_MSK_IAM = mechanism, aws_secret_key_id, aws_secret_access_key, aws_region} = _sasl_opts
      )
      when is_binary(aws_secret_key_id) and is_binary(aws_secret_access_key) do
    case handshake(sock, mod, timeout, client_id, mechanism, @handshake_version) do
      :ok ->
        client_final_msg =
          SignedPayloadGenerator.get_msk_signed_payload(
            host,
            DateTime.utc_now(),
            aws_region,
            aws_secret_key_id,
            aws_secret_access_key
          )

        server_final_msg = send_recv(sock, mod, client_id, timeout, client_final_msg)

        case :kpro_lib.find(:error_code, server_final_msg) do
          :no_error -> :ok
          other -> {:error, other}
        end

      error ->
        Logger.error("Handshake failed #{error}")
        {:error, error}
    end
  end

  def auth(_host, _sock, _mod, _client_id, _timeout, _sasl_opts) do
    {:error, "Invalid SASL mechanism"}
  end

  @impl true
  def auth(host, sock, _vsn, mod, client_id, timeout, sasl_opts) do
    # Delegate to the existing auth/6 implementation
    auth(host, sock, mod, client_id, timeout, sasl_opts)
  end

  defp send_recv(sock, mod, client_id, timeout, payload) do
    req = :kpro_req_lib.make(:sasl_authenticate, _auth_req_vsn = 0, [{:auth_bytes, payload}])
    rsp = :kpro_lib.send_and_recv(req, sock, mod, client_id, timeout)

    Logger.debug("Final Auth Response from server - #{inspect(rsp)}")

    rsp
  end

  defp cs([]), do: "[]"
  defp cs([x]), do: x
  defp cs([h | t]), do: [h, "," | cs(t)]

  defp handshake(sock, mod, timeout, client_id, mechanism, vsn) do
    req = :kpro_req_lib.make(:sasl_handshake, vsn, [{:mechanism, mechanism}])
    rsp = :kpro_lib.send_and_recv(req, sock, mod, client_id, timeout)
    error_code = :kpro_lib.find(:error_code, rsp)

    Logger.debug("Error Code field in initial handshake response : #{error_code}")

    case error_code do
      :no_error ->
        :ok

      :unsupported_sasl_mechanism ->
        enabled_mechanisms = :kpro_lib.find(:mechanisms, rsp)
        "sasl mechanism #{mechanism} is not enabled in kafka, "
        "enabled mechanism(s): #{cs(enabled_mechanisms)}"

      other ->
        other
    end
  end
end
