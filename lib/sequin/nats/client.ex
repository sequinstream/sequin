defmodule Sequin.Nats.Client do
  @moduledoc false
  @behaviour Sequin.Nats

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.NatsSink
  alias Sequin.Error
  alias Sequin.Nats.ConnectionCache
  alias Sequin.NetworkUtils

  @impl Sequin.Nats
  def send_messages(%NatsSink{} = sink, messages) when is_list(messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      Enum.reduce_while(messages, :ok, fn message, :ok ->
        case publish_message(message, connection, sink) do
          :ok ->
            {:cont, :ok}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  @impl Sequin.Nats
  def test_connection(%NatsSink{} = sink) do
    with :ok <-
           NetworkUtils.test_tcp_reachability(sink.host, sink.port, NatsSink.ipv6?(sink), :timer.seconds(10)),
         {:ok, connection} <- ConnectionCache.connection(sink) do
      nuid = 12 |> :crypto.strong_rand_bytes() |> Base.encode64()
      subject = "_SEQUIN.TEST.#{nuid}"
      payload = "ping"

      with {:ok, subscription} <- Gnat.sub(connection, self(), subject),
           :ok <- Gnat.unsub(connection, subscription, max_messages: 1),
           :ok <- Gnat.pub(connection, subject, payload) do
        receive do
          {:msg, %{topic: ^subject, body: ^payload}} ->
            :ok
        after
          5000 ->
            {:error, Error.service(service: :nats, message: "Failed to verify NATS connection")}
        end
      else
        _ -> {:error, Error.service(service: :nats, message: "Failed to verify NATS connection")}
      end
    end
  catch
    :exit, error ->
      {:error, to_sequin_error(error)}
  end

  defp publish_message(message, connection, sink) do
    opts = [headers: get_headers(message)]
    payload = to_payload(message)

    try do
      Gnat.pub(connection, sink.subject, Jason.encode_to_iodata!(payload), opts)
    catch
      error ->
        {:error, to_sequin_error(error)}
    end
  end

  defp to_sequin_error(error) do
    case error do
      error when is_binary(error) ->
        Error.service(service: :nats, message: "NATS error: #{error}")

      _ ->
        Error.service(service: :nats, message: "Unknown NATS error")
    end
  end

  defp to_payload(%ConsumerEvent{} = message) do
    %{
      record: message.data.record,
      metadata: message.data.metadata
    }
  end

  defp to_payload(%ConsumerRecord{} = message) do
    %{
      record: message.data.record,
      changes: message.data.changes,
      action: message.data.action,
      metadata: message.data.metadata
    }
  end

  defp get_headers(%ConsumerEvent{} = message) do
    [{"Nats-Msg-Id", to_string(message.id)}]
  end

  defp get_headers(%ConsumerRecord{} = message) do
    [{"Nats-Msg-Id", to_string(message.id)}]
  end
end
