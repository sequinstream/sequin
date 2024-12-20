defmodule Sequin.Sinks.Kafka.AwsMskIam.SignedPayloadGenerator do
  @moduledoc """
  AwsSignatureLib module is a facade behavior/implementation for Erlang's aws_signature module
  Ref: https://github.com/kafka4beam/kafka_protocol
  Contains wrapper functions for methods in aws_signature module
  Purpose: Creating this as a behavior helps us mock the payload building and signing calls made
  """

  @callback get_msk_signed_payload(binary(), DateTime.t(), binary(), binary()) :: binary()

  # TODO: Make service, region, user_agent, version and ttl runtime configurable
  @service "kafka-cluster"
  @method "GET"
  @version "2020_10_22"
  @user_agent "msk-elixir-client"
  # 15 minutes
  @ttl 900

  @doc """
  Builds AWS4 signed AWS_MSK_IAM payload needed for making SASL authentication request with broker
  # Reference: https://github.com/aws-beam/aws_signature

  Returns signed payload in bytes
  """
  def get_msk_signed_payload(host, now, aws_region, aws_secret_key_id, aws_secret_access_key)
      when is_binary(aws_secret_key_id) and is_binary(aws_secret_access_key) do
    url = "kafka://" <> to_string(host) <> "?Action=kafka-cluster%3AConnect"

    aws_v4_signed_query =
      :aws_signature.sign_v4_query_params(
        aws_secret_key_id,
        aws_secret_access_key,
        aws_region,
        @service,
        # Formats to {{now.year, now.month, now.day}, {now.hour, now.minute, now.second}}
        NaiveDateTime.to_erl(now),
        @method,
        url,
        ttl: @ttl
      )

    url_map = :aws_signature_utils.parse_url(aws_v4_signed_query)

    # Convert query params into a map with keys downcased and values decoded
    signed_payload =
      url_map[:query]
      |> URI.query_decoder()
      |> Map.new(fn {k, v} -> {String.downcase(k), URI.decode(v)} end)

    # Building rest of the payload in the format from Java reference implementation
    signed_payload =
      signed_payload
      |> Map.put("version", @version)
      |> Map.put("host", url_map[:host])
      |> Map.put("user-agent", @user_agent)

    Jason.encode!(signed_payload)
  end
end
