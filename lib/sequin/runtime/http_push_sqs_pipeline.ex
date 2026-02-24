defmodule Sequin.Runtime.HttpPushSqsPipeline do
  @moduledoc """
  Broadway pipeline for processing SQS messages containing binary-encoded ConsumerEvent objects.
  This pipeline is used when HttpPushSink is configured with via_sqs set to true.

  This pipeline will not run unless the HTTP_PUSH_VIA_SQS_QUEUE_URL environment variable is set.

  It will:
  1. Decode Base64 and deserialize the binary data to get the ConsumerEvent
  2. Fetch the consumer and HTTP endpoint
  3. Transform the message data
  4. Send the HTTP request to the target endpoint
  """
  use Broadway

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Error.ServiceError
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Prometheus
  alias Sequin.Runtime.Trace
  alias Sequin.Transforms

  require Logger

  @backoff_base to_timeout(minute: 1)
  @backoff_max to_timeout(minute: 10)

  @spec main_queue_child_spec(keyword()) :: Supervisor.child_spec() | nil
  def main_queue_child_spec(opts \\ []) do
    queue_url = Map.fetch!(fetch_sqs_config!(), :main_queue_url)
    name = Keyword.get(opts, :name, __MODULE__)

    opts =
      opts
      |> Keyword.put(:queue_url, queue_url)
      |> Keyword.put(:queue_kind, :main)
      |> Keyword.put(:name, name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @spec dlq_child_spec(keyword()) :: Supervisor.child_spec() | nil
  def dlq_child_spec(opts \\ []) do
    queue_url = Map.fetch!(fetch_sqs_config!(), :dlq_url)
    name = Keyword.get(opts, :name, Module.concat(__MODULE__, Dlq))

    opts =
      opts
      |> Keyword.put(:queue_url, queue_url)
      |> Keyword.put(:queue_kind, :dlq)
      |> Keyword.put(:name, name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def enabled? do
    case fetch_sqs_config() do
      {:ok, via} when is_map(via) -> true
      _ -> false
    end
  end

  def setting_worker_concurrency, do: Keyword.get(config(), :worker_concurrency, 1000)

  def setting_producer_concurrency, do: Keyword.get(config(), :producer_concurrency, 10)

  def set_setting_worker_concurrency(setting_worker_concurrency) do
    config = Keyword.put(config(), :worker_concurrency, setting_worker_concurrency)
    Application.put_env(:sequin, __MODULE__, config)
  end

  def set_setting_producer_concurrency(setting_producer_concurrency) do
    config = Keyword.put(config(), :producer_concurrency, setting_producer_concurrency)
    Application.put_env(:sequin, __MODULE__, config)
  end

  def config do
    Sequin.get_env(:sequin, __MODULE__)
  end

  def fetch_sqs_config do
    Keyword.fetch(config(), :sqs)
  end

  def fetch_sqs_config! do
    Keyword.fetch!(config(), :sqs)
  end

  @doc """
  Starts the Broadway pipeline.

  This is called by the application supervisor if SQS configuration is available.
  """
  def start_link(opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    queue_kind = Keyword.fetch!(opts, :queue_kind)
    name = Keyword.fetch!(opts, :name)

    {:ok, sqs_config} = fetch_sqs_config()
    region = Map.fetch!(sqs_config, :region)

    {access_key_id, secret_access_key, token} =
      if Map.get(sqs_config, :use_task_role) do
        # Use aws_credentials provider chain
        case :aws_credentials.get_credentials() do
          :undefined ->
            raise "Task role credentials not found"

          credentials ->
            {credentials.access_key_id, credentials.secret_access_key, credentials[:token]}
        end
      else
        # Use explicit credentials
        {Map.fetch!(sqs_config, :access_key_id), Map.fetch!(sqs_config, :secret_access_key), nil}
      end

    producer_mod = Keyword.get(opts, :producer_mod, BroadwaySQS.Producer)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {
          producer_mod,
          queue_url: queue_url,
          config:
            maybe_put_token([access_key_id: access_key_id, secret_access_key: secret_access_key, region: region], token),
          attribute_names: [:sent_timestamp, :approximate_receive_count, :approximate_first_receive_timestamp],
          receive_interval: 1_000,
          max_number_of_messages: 10,
          wait_time_seconds: 1,
          visibility_timeout: 60
        },
        concurrency: setting_producer_concurrency()
      ],
      processors: [
        default: [
          concurrency: setting_worker_concurrency()
        ]
      ],
      context: %{
        test_pid: Keyword.get(opts, :test_pid),
        queue_kind: queue_kind
      }
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: data} = message, context) do
    %{queue_kind: queue_kind} = context
    setup_allowances(context)

    # The message body is a JSON string with a "data" field containing Base64-encoded binary
    %{"data" => base64_data} = Jason.decode!(data)

    # Decode Base64 and deserialize to get the ConsumerEvent
    binary_data = Base.decode64!(base64_data)
    %ConsumerEvent{} = consumer_event = binary_data |> :erlang.binary_to_term([:safe]) |> ConsumerEvent.struct_from_map()

    consumer_id = consumer_event.consumer_id
    Logger.metadata(consumer_id: consumer_id)

    # Fetch the consumer and preload the HTTP endpoint
    case fetch_consumer(consumer_id) do
      {:ok, consumer} ->
        Prometheus.increment_http_via_sqs_message_deliver_attempt_count(consumer_id, consumer.name)
        {latency_us, result} = :timer.tc(&deliver_to_http_endpoint/3, [consumer, consumer_event, queue_kind])

        # Is `1` on first delivery
        %{metadata: %{attributes: %{"approximate_receive_count" => receive_count}}} = message

        final_delivery? =
          if Keyword.fetch!(config(), :discards_disabled?) do
            false
          else
            receive_count - 1 >= consumer.max_retry_count
          end

        case result do
          {:ok, %Req.Response{}} ->
            # Track success metrics
            Prometheus.increment_http_via_sqs_message_success_count(consumer_id, consumer.name)
            Prometheus.observe_http_via_sqs_message_deliver_latency_us(consumer_id, consumer.name, latency_us)

            if ingested_at = consumer_event.ingested_at do
              total_latency = DateTime.diff(DateTime.utc_now(), ingested_at, :microsecond)

              Prometheus.observe_http_via_sqs_message_total_latency_us(
                consumer_id,
                consumer.name,
                queue_kind,
                total_latency
              )
            end

            message

          {:error, error} ->
            Logger.warning("[HttpPushSqsPipeline] Failed to deliver message to HTTP endpoint: #{inspect(error)}")

            Health.put_event(consumer, %Health.Event{slug: :http_via_sqs_delivery, status: :fail})

            if final_delivery? do
              Logger.warning("[HttpPushSqsPipeline] Discarding message after #{consumer.max_retry_count} retries")

              Trace.warning(consumer_id, %Trace.Event{
                message: "Discarding message after #{consumer.max_retry_count} retries"
              })

              Prometheus.increment_http_via_sqs_message_discard_count(consumer_id, consumer.name)

              Trace.error(consumer_id, %Trace.Event{
                message: "Discarding message after max retries",
                error: error,
                extra: %{
                  max_retry_count: consumer.max_retry_count
                }
              })

              message
            else
              # Calculate exponential backoff timeout based on receive count
              # Start at 1 minute, max at 10 minutes
              %{metadata: %{attributes: %{"approximate_receive_count" => receive_count}}} = message
              retry_count = receive_count - 1
              backoff_ms = Sequin.Time.exponential_backoff(@backoff_base, retry_count, @backoff_max)
              backoff_seconds = div(backoff_ms, 1000)

              Logger.info(
                "[HttpPushSqsPipeline] Nacking message with #{backoff_seconds} seconds visibility timeout (retry #{retry_count})"
              )

              Trace.info(consumer_id, %Trace.Event{
                message: "Nacking message with exponential backoff",
                extra: %{
                  retry_count: retry_count,
                  backoff_seconds: backoff_seconds,
                  error: error
                }
              })

              # Nack the message with the calculated backoff time
              message
              |> Message.configure_ack(on_failure: {:nack, backoff_seconds})
              |> Message.failed("Failed to deliver message: #{inspect(error)}")
            end
        end

      {:error, %NotFoundError{entity: :sink_consumer}} ->
        Logger.info("[HttpPushSqsPipeline] Consumer not found, skipping")
        message

      {:error, %NotFoundError{entity: :http_endpoint}} ->
        Logger.warning("[HttpPushSqsPipeline] HTTP endpoint not found, marking message as failed")
        Message.failed(message, "HTTP endpoint not found")

      {:error, :disabled} ->
        Logger.info("[HttpPushSqsPipeline] Consumer is disabled, skipping")
        message
    end
  end

  # Fetch the consumer and preload the HTTP endpoint
  defp fetch_consumer(consumer_id) do
    case Consumers.get_cached_consumer(consumer_id) do
      {:ok, %SinkConsumer{status: :disabled}} ->
        {:error, :disabled}

      {:ok, %SinkConsumer{} = consumer} ->
        SinkConsumer.preload_cached_http_endpoint(consumer)

      {:error, %NotFoundError{}} = error ->
        error
    end
  end

  # Deliver the message to the HTTP endpoint
  defp deliver_to_http_endpoint(consumer, consumer_event, queue_kind) do
    http_endpoint = consumer.sink.http_endpoint

    # Transform the message data
    transformed_data = Transforms.Message.to_external(consumer, consumer_event)

    # TODO Migrate to new routing mechanism

    # Prepare HTTP request
    headers =
      (http_endpoint.headers || %{})
      |> Map.merge(http_endpoint.encrypted_headers || %{})
      |> Map.put("x-sequin-via", "sqs")

    req =
      [
        method: "POST",
        base_url: HttpEndpoint.url(http_endpoint),
        url: consumer.sink.http_endpoint_path || "",
        headers: headers,
        json: transformed_data,
        receive_timeout: consumer.ack_wait_ms || 30_000,
        finch: Sequin.Finch
      ]
      |> Req.new()
      |> Req.merge(default_req_opts())

    Trace.info(consumer.id, %Trace.Event{
      message: "Fetched from SQS, making HTTP request (queue=#{queue_kind})",
      req_request: req,
      extra: %{
        transformed_data: transformed_data
      }
    })

    # Make the HTTP request
    with {:ok, resp} <- Req.request(req),
         :ok <- ensure_status(resp, consumer) do
      Trace.info(consumer.id, %Trace.Event{
        message: "Message delivered to HTTP endpoint",
        req_request: req,
        req_response: resp
      })

      {:ok, resp}
    else
      {:error, %ServiceError{} = error} ->
        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver message to HTTP endpoint",
          req_request: req,
          error: error
        })

        {:error, error}

      {:error, %Mint.TransportError{reason: reason} = error} ->
        Prometheus.increment_http_via_sqs_message_deliver_failure_count(consumer.id, consumer.name, reason)

        Logger.warning(
          "[HttpPushSqsPipeline] #{req.method} to webhook endpoint failed with Mint.TransportError: #{Exception.message(error)}",
          error: error
        )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver message to HTTP endpoint",
          req_request: req,
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
        Prometheus.increment_http_via_sqs_message_deliver_failure_count(consumer.id, consumer.name, reason)

        Logger.warning(
          "[HttpPushSqsPipeline] #{req.method} to webhook endpoint failed with Req.TransportError: #{Exception.message(error)}",
          error: error
        )

        Trace.error(consumer.id, %Trace.Event{
          message: "Failed to deliver message to HTTP endpoint",
          req_request: req,
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
          message: "Failed to deliver message to HTTP endpoint",
          req_request: req,
          error: error
        })

        {:error, error}
    end
  end

  # Check if the HTTP response status is successful
  defp ensure_status(%Req.Response{} = response, %SinkConsumer{} = consumer) do
    if response.status in 200..299 do
      Metrics.incr_http_endpoint_throughput(consumer.sink.http_endpoint)
      Health.put_event(consumer, %Health.Event{slug: :http_via_sqs_delivery, status: :success})
      :ok
    else
      Prometheus.increment_http_via_sqs_message_deliver_failure_count(consumer.id, consumer.name, response.status)

      {:error,
       Error.service(
         service: :http_endpoint,
         code: "bad_status",
         message: "Unexpected status code: #{response.status}",
         details: %{status: response.status, body: response.body}
       )}
    end
  end

  defp setup_allowances(%{test_pid: nil}), do: :ok

  defp setup_allowances(%{test_pid: test_pid}) do
    Req.Test.allow(__MODULE__, test_pid, self())
    Sandbox.allow(Sequin.Repo, test_pid, self())
    Mox.allow(Sequin.TestSupport.ApplicationMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end

  defp default_req_opts do
    Application.get_env(:sequin, __MODULE__)[:req_opts] || []
  end

  defp maybe_put_token(config, nil), do: config
  defp maybe_put_token(config, token), do: Keyword.put(config, :token, token)
end
