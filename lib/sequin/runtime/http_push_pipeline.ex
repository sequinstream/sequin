defmodule Sequin.Runtime.HttpPushPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.SQS
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Error.ServiceError
  alias Sequin.Functions.MiniElixir
  alias Sequin.Metrics
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.Trace
  alias Sequin.Transforms

  require Logger

  defmodule RoutingInfo do
    @moduledoc false
    @derive Jason.Encoder
    defstruct method: "POST", endpoint_path: ""
  end

  @impl SinkPipeline
  def init(context, opts) do
    consumer = Map.fetch!(context, :consumer)
    consumer = SinkConsumer.preload_http_endpoint(consumer)
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

  # Helper function to create AWS SQS client
  defp create_aws_client(config) do
    %{access_key_id: access_key_id, secret_access_key: secret_access_key, region: region} = config

    access_key_id
    |> AWS.Client.create(secret_access_key, region)
    |> HttpClient.put_client()
  end

  @impl SinkPipeline
  def apply_routing(_consumer, rinfo) when is_map(rinfo) do
    struct!(RoutingInfo, rinfo)
  rescue
    KeyError ->
      expected_keys =
        RoutingInfo.__struct__()
        |> Map.keys()
        |> Enum.reject(&(&1 == :__struct__))
        |> Enum.join(", ")

      raise Error.invariant(
              message: "Invalid routing response. Expected a map with keys: #{expected_keys}, got: #{inspect(rinfo)}"
            )
  end

  def apply_routing(_, v), do: raise("Routing function must return a map! Got: #{inspect(v)}")

  @impl SinkPipeline
  def handle_message(message, context) do
    routing = context.consumer.routing

    if is_nil(routing) do
      {:ok, message, context}
    else
      res = MiniElixir.run_compiled(routing, message.data.data)
      message = Broadway.Message.put_batch_key(message, apply_routing(context.consumer, res))
      {:ok, message, context}
    end
  end

  defguardp uses_via_sqs?(context) when context.consumer.sink.via_sqs == true

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) when uses_via_sqs?(context) do
    %{
      consumer: consumer,
      http_endpoint: http_endpoint,
      req_opts: req_opts,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    case push_to_sqs(http_endpoint, consumer, messages, req_opts, context) do
      :ok ->
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
      features: features,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    message_data =
      messages
      |> Enum.map(& &1.data)
      |> prepare_message_data(consumer, features)

    req_opts =
      case batch_info.batch_key do
        :default ->
          req_opts

        m when is_map(m) ->
          req_opts
          |> Keyword.put(:url, m.endpoint_path)
          |> Keyword.put(:method, m.method)
      end

    case push_message(http_endpoint, consumer, message_data, req_opts) do
      :ok ->
        Metrics.incr_http_endpoint_throughput(http_endpoint)
        {:ok, messages, context}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  defp prepare_message_data(messages, consumer, features) do
    cond do
      features[:legacy_event_transform] && length(messages) == 1 ->
        [message] = messages
        legacy_event_transform_message(consumer, message.data)

      features[:legacy_event_singleton_transform] && length(messages) == 1 ->
        [message] = messages
        message.data

      consumer.sink.batch == false ->
        [message] = messages
        Transforms.Message.to_external(consumer, message)

      true ->
        %{data: Enum.map(messages, &Transforms.Message.to_external(consumer, &1))}
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

  defp push_message(%HttpEndpoint{} = http_endpoint, %SinkConsumer{} = consumer, message_data, req_opts) do
    headers = http_endpoint.headers
    encrypted_headers = http_endpoint.encrypted_headers || %{}
    headers = Map.merge(headers, encrypted_headers)

    req =
      [
        method: "POST",
        base_url: HttpEndpoint.url(http_endpoint),
        url: consumer.sink.http_endpoint_path || "",
        headers: headers,
        json: message_data,
        receive_timeout: consumer.ack_wait_ms,
        finch: Sequin.Finch
      ]
      |> Keyword.merge(req_opts)
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
          error: error
        })

        {:error, error}

      {:error, %Mint.TransportError{reason: reason} = error} ->
        Logger.error(
          "[HttpPushPipeline] #{req.method} to webhook endpoint failed with Mint.TransportError: #{Exception.message(error)}",
          error: error
        )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver messages to HTTP endpoint [transport error]",
          error: error
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
          error: error
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
          error: error
        })

        {:error, error}
    end
  end

  # Handle pushing messages to SQS when via configuration is present
  defp push_to_sqs(%HttpEndpoint{} = _http_endpoint, %SinkConsumer{} = consumer, messages, _req_opts, context) do
    Logger.debug("[HttpPushPipeline] Pushing #{length(messages)} messages to SQS for consumer #{consumer.id}")

    %{sqs_client: sqs_client} = context

    # Extract the raw ConsumerEvent data from each Broadway.Message
    # and convert to binary using erlang.term_to_binary
    sqs_messages =
      Enum.map(messages, fn %Broadway.Message{} = message ->
        # Extract the ConsumerEvent from the Broadway.Message
        %ConsumerEvent{} = consumer_event = message.data

        # Convert to binary
        binary_data = consumer_event |> ConsumerEvent.map_from_struct() |> :erlang.term_to_binary() |> Base.encode64()

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

    case SQS.send_messages(sqs_client, sqs_config.queue_url, sqs_messages) do
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

        :ok

      {:error, error} ->
        Logger.error("[HttpPushPipeline] Failed to send message to SQS: #{inspect(error)}")

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to send messages to SQS",
          error: error
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
end
