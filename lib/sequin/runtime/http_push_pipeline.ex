defmodule Sequin.Runtime.HttpPushPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Metrics
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Transforms.MiniElixir

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

    context
    |> Map.put(:consumer, consumer)
    |> Map.put(:http_endpoint, consumer.sink.http_endpoint)
    |> Map.put(:req_opts, req_opts)
    |> Map.put(:features, features)
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

  @impl SinkPipeline
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

      true ->
        %{data: Enum.map(messages, &Sequin.Transforms.Message.to_external(consumer, &1))}
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
        receive_timeout: consumer.ack_wait_ms
      ]
      |> Keyword.merge(req_opts)
      |> Req.new()

    case Req.request(req) do
      {:ok, response} ->
        ensure_status(response, consumer)

      {:error, %Mint.TransportError{reason: reason} = error} ->
        Logger.error(
          "[HttpPushPipeline] #{req.method} to webhook endpoint failed with Mint.TransportError: #{Exception.message(error)}",
          error: error
        )

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

        {:error,
         Error.service(
           service: :http_endpoint,
           code: "transport_error",
           message: "#{req.method} to webhook endpoint failed",
           details: reason
         )}

      {:error, reason} ->
        {:error,
         Error.service(service: :http_endpoint, code: "unknown_error", message: "Request failed", details: reason)}
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
  end
end
