defmodule Sequin.Runtime.HttpPushPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.SQS
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Error.ServiceError
  alias Sequin.Metrics
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.Trace
  alias Sequin.Transforms

  require Logger

  @impl SinkPipeline
  def init(context, opts) do
    consumer = Map.fetch!(context, :consumer)
    consumer = SinkConsumer.preload_http_endpoint!(consumer)

    # TODO: Do this without database (or at least without tables)
    consumer = Sequin.Repo.preload(consumer, :postgres_database)
    consumer = put_in(consumer.postgres_database.tables, [])
    :erlang.garbage_collect()

    req_opts = Keyword.get(opts, :req_opts, [])
    features = Keyword.get(opts, :features, [])

    context =
      context
      |> Map.put(:consumer, consumer)
      |> Map.put(:http_endpoint, consumer.sink.http_endpoint)
      |> Map.put(:req_opts, req_opts)
      |> Map.put(:features, features)

    # Set up SQS client if the sink has via_sqs enabled
    if consumer.sink.via_sqs do
      # Get SQS configuration directly
      case Sequin.Runtime.HttpPushSqsPipeline.fetch_sqs_config() do
        {:ok, sqs_config} ->
          # Create SQS client with the configuration
          sqs_client = create_aws_client(sqs_config)
          # Store the config in context for SQS operations
          context
          |> Map.put(:sqs_config, sqs_config)
          |> Map.put(:sqs_client, sqs_client)

        _ ->
          # If no SQS config available, continue without SQS
          Logger.warning("SQS configuration not found despite via_sqs being enabled")
          context
      end
    else
      context
    end
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{sink: %HttpPushSink{via_sqs: false}}) do
    []
  end

  def batchers_config(%SinkConsumer{sink: %HttpPushSink{via_sqs: true}}) do
    [
      default: [
        concurrency: min(System.schedulers_online() * 2, 80),
        batch_size: 10,
        batch_timeout: 1
      ]
    ]
  end

  # Helper function to create AWS SQS client
  defp create_aws_client(config) do
    %{access_key_id: access_key_id, secret_access_key: secret_access_key, region: region} = config

    access_key_id
    |> AWS.Client.create(secret_access_key, region)
    |> HttpClient.put_client()
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    consumer = context.consumer
    routing_info = Routing.route_message(consumer, message)
    message = Broadway.Message.put_batch_key(message, routing_info)

    {:ok, message, context}
  end

  defguardp uses_via_sqs?(context) when context.consumer.sink.via_sqs == true

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) when uses_via_sqs?(context) do
    %{consumer: consumer, test_pid: test_pid} = context

    setup_allowances(test_pid)

    case push_to_sqs(consumer, messages, context) do
      {:ok, messages} ->
        {:ok, messages, context}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  def handle_batch(:default, messages, batch_info, context) do
    %{
      consumer: consumer,
      http_endpoint: http_endpoint,
      req_opts: req_opts,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    consumer_messages = Enum.map(messages, & &1.data)
    consumer_messages = Consumers.enrich_messages!(consumer.postgres_database, consumer.enrichment, consumer_messages)

    routed_message =
      %Routing.RoutedMessage{
        routing_info: batch_info.batch_key,
        transformed_message: prepare_message_data(consumer_messages, consumer)
      }

    case push_message(http_endpoint, consumer, routed_message, req_opts) do
      :ok ->
        Metrics.incr_http_endpoint_throughput(http_endpoint)
        {:ok, messages, context}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  defp prepare_message_data(messages, consumer) do
    if consumer.sink.batch == false do
      [message] = messages
      Transforms.Message.to_external(consumer, message)
    else
      %{data: Enum.map(messages, &Transforms.Message.to_external(consumer, &1))}
    end
  end

  defp push_message(%HttpEndpoint{} = http_endpoint, %SinkConsumer{} = consumer, routed_message, req_opts) do
    # TODO Consider moving all header merging into HttpPush routing
    headers = http_endpoint.headers
    encrypted_headers = http_endpoint.encrypted_headers || %{}
    headers = Map.merge(headers, encrypted_headers)
    headers = Map.merge(headers, routed_message.routing_info.headers)
    headers = Map.merge(headers, Map.new(Keyword.get(req_opts, :headers, [])))

    # Check if gzip compression should be enabled based on content-encoding header
    gzip_compress_body = should_gzip_compress_body?(headers)

    req =
      [
        method: routed_message.routing_info.method,
        base_url: HttpEndpoint.url(http_endpoint),
        url: routed_message.routing_info.endpoint_path,
        headers: headers,
        json: routed_message.transformed_message,
        receive_timeout: consumer.ack_wait_ms,
        finch: Sequin.Finch,
        compress_body: gzip_compress_body
      ]
      |> Keyword.merge(Keyword.drop(req_opts, [:method, :base_url, :url, :headers, :json, :receive_timeout, :finch]))
      |> Req.new()

    with {:ok, resp} <- Req.request(req),
         :ok <- ensure_status(resp, consumer) do
      Trace.info(consumer.id, %Trace.Event{
        message: "Messages delivered to HTTP endpoint",
        req_request: req,
        req_response: resp
      })

      :ok
    else
      {:error, %ServiceError{} = error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver messages to HTTP endpoint",
          error: error,
          req_request: req
        })

        {:error, error}

      {:error, %Mint.TransportError{reason: reason} = error} ->
        Logger.error(
          "[HttpPushPipeline] #{req.method} to webhook endpoint failed with Mint.TransportError: #{Exception.message(error)}",
          error: error,
          req_request: req
        )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver messages to HTTP endpoint [transport error]",
          error: error,
          req_request: req
        })

        {:error,
         Error.service(
           service: :http_endpoint,
           code: "transport_error",
           message: "#{req.method} to webhook endpoint failed",
           details: reason
         )}

      {:error, %Req.TransportError{reason: reason} = error} ->
        Logger.error(
          "[HttpPushPipeline] #{req.method} to webhook endpoint failed with Req.TransportError: #{Exception.message(error)}",
          error: error
        )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver messages to HTTP endpoint [transport error]",
          error: error,
          req_request: req
        })

        {:error,
         Error.service(
           service: :http_endpoint,
           code: "transport_error",
           message: "#{req.method} to webhook endpoint failed",
           details: reason
         )}

      {:error, reason} ->
        error = Error.service(service: :http_endpoint, code: "unknown_error", message: "Request failed", details: reason)

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver messages to HTTP endpoint",
          error: error,
          req_request: req
        })

        {:error, error}
    end
  end

  @invalid_message_or_request_size_error [:invalid_message_contents, :batch_request_too_long]

  # Handle pushing messages to SQS when via configuration is present
  defp push_to_sqs(%SinkConsumer{} = consumer, messages, context) do
    Logger.debug("[HttpPushPipeline] Pushing #{length(messages)} messages to SQS for consumer #{consumer.id}")
    message_count = length(messages)

    %{sqs_client: sqs_client} = context

    # Extract the raw ConsumerEvent data from each Broadway.Message
    # and convert to binary using erlang.term_to_binary
    sqs_messages =
      Enum.map(messages, fn %Broadway.Message{} = message ->
        # Extract the ConsumerEvent from the Broadway.Message
        %ConsumerEvent{} = consumer_event = message.data

        # Convert to binary
        binary_data =
          consumer_event
          |> ConsumerEvent.map_from_struct()
          |> :erlang.term_to_binary(compressed: 6)
          |> Base.encode64()

        # Build SQS message with binary data as the message body
        %{
          message_body: %{data: binary_data},
          id: UUID.uuid4()
        }
      end)

    # Add deduplication ID if the queue is FIFO
    # This would need to be applied to each message in the batch
    # queue_is_fifo = consumer.sink.via && String.ends_with?(consumer.sink.via.queue_url, ".fifo")
    # sqs_messages =
    #   if queue_is_fifo do
    #     Enum.map(sqs_messages, fn sqs_message ->
    #       deduplication_id = UUID.uuid4()
    #       Map.merge(sqs_message, %{
    #         message_deduplication_id: deduplication_id,
    #         message_group_id: consumer.id
    #       })
    #     end)
    #   else
    #     sqs_messages
    #   end

    # Send to SQS - now sending multiple messages in a batch
    %{sqs_config: sqs_config} = context

    case SQS.send_messages(sqs_client, sqs_config.main_queue_url, sqs_messages) do
      :ok ->
        Logger.debug(
          "[HttpPushPipeline] Successfully routed HTTP request to SQS via configuration for consumer #{consumer.id}"
        )

        Trace.info(consumer.id, %Trace.Event{
          message: "Sent messages to SQS",
          extra: %{
            sqs_messages: sqs_messages
          }
        })

        {:ok, messages}

      {:error, %Error.ServiceError{code: code}}
      when code in @invalid_message_or_request_size_error and message_count == 1 ->
        Logger.info("[HttpPushPipeline] Discarding message due to SQS error: #{code}")
        {:ok, messages}

      {:error, %Error.ServiceError{code: code}} when code in @invalid_message_or_request_size_error ->
        Logger.info("[HttpPushPipeline] Batch was rejected due to SQS error: #{code}. Retrying messages individually.")

        Enum.reduce_while(messages, {:ok, []}, fn message, {:ok, messages} ->
          case push_to_sqs(consumer, [message], context) do
            {:ok, [_msg]} -> {:cont, {:ok, [message | messages]}}
            {:error, error} -> {:halt, {:error, error}}
          end
        end)

      {:error, error} ->
        Logger.error("[HttpPushPipeline] Failed to send message to SQS: #{inspect(error)}")

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to send messages to SQS",
          error: error,
          extra: %{
            sqs_messages: sqs_messages
          }
        })

        {:error, Error.service(service: :sqs, message: "Failed to send to SQS", details: error)}
    end
  end

  # TODO: Temp fix for flaky sink consumer
  defp ensure_status(%Req.Response{} = response, %SinkConsumer{id: "52f95f90-4e22-4b44-96d6-438d9b29661d"}) do
    if response.status in 200..299 or response.status == 413 do
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

  defp ensure_status(%Req.Response{} = response, _consumer) do
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

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Req.Test.allow(HttpClient, test_pid, self())
  end

  # Check if the body should be gzip compressed based on content-encoding header
  defp should_gzip_compress_body?(headers) do
    # Find content-encoding header in a case-insensitive way
    content_encoding =
      Enum.find_value(headers, fn
        {key, value} when is_binary(key) ->
          if String.downcase(key) == "content-encoding", do: value

        {key, value} when is_atom(key) ->
          if String.downcase(Atom.to_string(key)) == "content-encoding", do: value

        _ ->
          nil
      end)

    case content_encoding do
      nil ->
        false

      value ->
        case String.downcase(value) do
          "gzip" ->
            true

          _ ->
            Logger.warning("[HttpPushPipeline] Content-Encoding #{value} not supported yet")
            false
        end
    end
  end
end
