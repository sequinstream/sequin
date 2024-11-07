defmodule Sequin.ConsumersRuntime.HttpPushPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo

  require Logger

  def start_link(opts) do
    %HttpPushConsumer{} = consumer = Keyword.fetch!(opts, :consumer)
    consumer = Repo.preload(consumer, [:http_endpoint])
    producer = Keyword.get(opts, :producer, Sequin.ConsumersRuntime.ConsumerProducer)
    req_opts = Keyword.get(opts, :req_opts, [])
    test_pid = Keyword.get(opts, :test_pid)
    features = Keyword.get(opts, :features, [])
    legacy_event_transform = features[:legacy_event_transform]
    legacy_event_singleton_transform = features[:legacy_event_singleton_transform]

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer, test_pid: test_pid]}
      ],
      processors: [
        default: [
          concurrency: consumer.max_waiting,
          max_demand: 1
        ]
      ],
      context: %{
        consumer: consumer,
        http_endpoint: consumer.http_endpoint,
        req_opts: req_opts,
        features: [
          legacy_event_transform: legacy_event_transform,
          legacy_event_singleton_transform: legacy_event_singleton_transform
        ]
      }
    )
  end

  def via_tuple(consumer_id) do
    Sequin.Registry.via_tuple({__MODULE__, consumer_id})
  end

  # Used by Broadway to name processes in topology according to our registry
  @impl Broadway
  def process_name({:via, Registry, {Sequin.Registry, {__MODULE__, id}}}, base_name) do
    Sequin.Registry.via_tuple({__MODULE__, {base_name, id}})
  end

  @impl Broadway
  def handle_message(_, %Broadway.Message{data: messages} = message, %{
        consumer: consumer,
        http_endpoint: http_endpoint,
        req_opts: req_opts,
        features: features
      }) do
    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id,
      http_endpoint_id: http_endpoint.id
    )

    message_data =
      cond do
        features[:legacy_event_transform] ->
          [message] = messages
          legacy_event_transform_message(consumer, message.data)

        features[:legacy_event_singleton_transform] ->
          [message] = messages
          message.data

        true ->
          %{data: Enum.map(messages, & &1.data)}
      end

    case push_message(http_endpoint, consumer, message_data, req_opts) do
      :ok ->
        # Temporary extended logging
        # Delete after webhook batch migration
        Logger.info("Pushed message successfully", message_data: message_data)

        Health.update(consumer, :push, :healthy)
        Metrics.incr_http_endpoint_throughput(http_endpoint)

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :info,
            consumer.account_id,
            msg.replication_message_trace_id,
            "Pushed message successfully"
          )
        end)

        message

      {:error, reason} ->
        Logger.warning("Failed to push message: #{inspect(reason)}")

        Health.update(consumer, :push, :error, reason)

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :error,
            consumer.account_id,
            msg.replication_message_trace_id,
            "Failed to push message: #{Exception.message(reason)}"
          )
        end)

        Broadway.Message.failed(message, reason)
    end
  end

  defp legacy_event_transform_message(consumer, message_data) do
    case message_data do
      %ConsumerRecordData{
        record: %{
          "action" => action,
          "changes" => changes,
          "committed_at" => committed_at,
          "record" => record,
          "source_table_name" => source_table_name,
          "source_table_schema" => source_table_schema
        }
      } ->
        %{
          "record" => record,
          "metadata" => %{
            "consumer" => %{
              "id" => consumer.id,
              "name" => consumer.name
            },
            "table_name" => source_table_name,
            "table_schema" => source_table_schema,
            "commit_timestamp" => committed_at
          },
          "action" => action,
          "changes" => changes
        }

      _ ->
        message_data
    end
  end

  defp push_message(%HttpEndpoint{} = http_endpoint, %HttpPushConsumer{} = consumer, message_data, req_opts) do
    headers = http_endpoint.headers
    encrypted_headers = http_endpoint.encrypted_headers || %{}
    headers = Map.merge(headers, encrypted_headers)

    req =
      [
        base_url: HttpEndpoint.url(http_endpoint),
        url: consumer.http_endpoint_path || "",
        headers: headers,
        json: message_data,
        receive_timeout: consumer.ack_wait_ms
      ]
      |> Keyword.merge(req_opts)
      |> Req.new()

    case Req.post(req) do
      {:ok, response} ->
        ensure_status(response)

      {:error, %Mint.TransportError{reason: reason} = error} ->
        Logger.error(
          "[HttpPushPipeline] POST to webhook endpoint failed with Mint.TransportError: #{Exception.message(error)}",
          error: error
        )

        {:error,
         Error.service(
           service: :http_endpoint,
           code: "transport_error",
           message: "POST to webhook endpoint failed",
           details: reason
         )}

      {:error, %Req.TransportError{reason: reason} = error} ->
        Logger.error(
          "[HttpPushPipeline] POST to webhook endpoint failed with Req.TransportError: #{Exception.message(error)}",
          error: error
        )

        {:error,
         Error.service(
           service: :http_endpoint,
           code: "transport_error",
           message: "POST to webhook endpoint failed",
           details: reason
         )}

      {:error, reason} ->
        {:error,
         Error.service(service: :http_endpoint, code: "unknown_error", message: "Request failed", details: reason)}
    end
  end

  defp ensure_status(%Req.Response{} = response) do
    if response.status in 200..299 do
      :ok
    else
      {:error,
       Error.service(
         service: :http_endpoint,
         code: "bad_status",
         message: "Unexpected status code: #{response.status}",
         details: %{status: response.status, body: response.body}
       )}
    end
  end
end
