defmodule Sequin.Sinks.Nats.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.Nats

  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.Runtime.Routing.RoutedMessage
  alias Sequin.Sinks.Nats
  alias Sequin.Sinks.Nats.ConnectionCache

  @impl Nats
  def send_messages(%SinkConsumer{sink: %NatsSink{} = sink} = consumer, messages) when is_list(messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      Enum.reduce_while(messages, :ok, fn message, :ok ->
        case publish_message(consumer, message, connection) do
          :ok ->
            {:cont, :ok}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  @test_timeout 5_000

  @impl Nats
  def test_connection(%NatsSink{} = sink) do
    with :ok <-
           NetworkUtils.test_tcp_reachability(sink.host, sink.port, NatsSink.ipv6?(sink), to_timeout(second: 10)),
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

          _ ->
            {:error,
             Error.service(
               service: :nats,
               message:
                 ~s(Failed to verify NATS connection: did not receive "#{payload}" on "#{subject}" subject. Verify NATS permissions are properly setup)
             )}
        after
          @test_timeout ->
            {:error,
             Error.service(
               service: :nats,
               message:
                 "Failed to verify NATS connection: did not receive test ping response after #{@test_timeout}ms. Verify NATS permissions are properly setup"
             )}
        end
      else
        _ ->
          {:error,
           Error.service(
             service: :nats,
             message:
               ~s(Failed to verify NATS connection: failed to send "#{payload}" to "#{subject}" subject. Verify NATS permissions are properly setup)
           )}
      end
    end
  catch
    :exit, error ->
      {:error, to_sequin_error(error)}
  end

  defp publish_message(
         %SinkConsumer{},
         %RoutedMessage{routing_info: %{subject: subject, headers: headers}, transformed_message: transformed_message},
         connection
       ) do
    list_headers =
      case headers do
        %{} ->
          Map.to_list(headers)

        headers when is_list(headers) ->
          headers

        _ ->
          raise "Invalid headers shape. Only maps and lists of tuples are supported. Got: #{inspect(headers)}"
      end

    opts = [headers: list_headers]

    try do
      Gnat.pub(connection, subject, Jason.encode_to_iodata!(transformed_message), opts)
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
end
