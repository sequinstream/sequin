defmodule Sequin.Sinks.S2.Client do
  @moduledoc false
  import Sequin.Error.Guards, only: [is_error: 1]

  alias S2.V1alpha.BasinService
  alias S2.V1alpha.StreamService
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.S2Sink
  alias Sequin.Error
  alias Sequin.Sinks.S2.ConnectionCache

  @spec send_messages(S2Sink.t(), [map()]) :: :ok | {:error, any()}
  def send_messages(%S2Sink{} = sink, messages) do
    with {:ok, channel} <- ConnectionCache.connection(sink) do
      messages =
        Enum.map(messages, fn message ->
          body =
            message
            |> to_body()
            |> Jason.encode!()

          %S2.V1alpha.AppendRecord{
            body: body,
            headers: [%S2.V1alpha.Header{name: "x-sequin-message-id", value: message.id}]
          }
        end)

      req = %S2.V1alpha.AppendRequest{
        input: %S2.V1alpha.AppendInput{
          stream: sink.stream,
          records: messages
        }
      }

      case StreamService.Stub.append(channel, req) do
        {:ok, _msg} ->
          :ok

        error ->
          handle_error(error, "send messages")
      end
    end
  end

  @spec test_connection(S2Sink.t()) :: :ok | {:error, any()}
  def test_connection(%S2Sink{} = sink) do
    with {:ok, channel} <- ConnectionCache.connection(sink) do
      req = %S2.V1alpha.GetStreamConfigRequest{stream: sink.stream}

      case BasinService.Stub.get_stream_config(channel, req) do
        {:ok, _response} ->
          :ok

        error ->
          handle_error(error, "test connection")
      end
    end
  end

  defp handle_error(error, req_desc) do
    case error do
      {:error, %GRPC.RPCError{} = res} ->
        {:error,
         Error.service(
           service: :s2,
           message: "Request failed: #{req_desc}, status=#{res.status} message=#{res.message}",
           details: res
         )}

      {:error, error} when is_error(error) ->
        {:error, error}

      {:error, error} ->
        {:error,
         Error.service(
           service: :s2,
           message: "Request failed: #{req_desc}",
           details: error
         )}
    end
  end

  defp to_body(%ConsumerEvent{} = message) do
    %{
      record: message.data.record,
      metadata: message.data.metadata
    }
  end

  defp to_body(%ConsumerRecord{} = message) do
    %{
      record: message.data.record,
      changes: message.data.changes,
      action: message.data.action,
      metadata: message.data.metadata
    }
  end
end
