defmodule Sequin.TestSupport.SimpleHttpServer do
  @moduledoc """
  This module is a naive single-threaded HTTP server for webhook testing.
  It accepts HTTP requests and forwards the parsed JSON body to the caller process.
  Only intended for use in test environments.
  """
  use GenServer

  require Logger

  @port Application.compile_env(:sequin, :webhook_test_port)
  @listen_options [:binary, packet: :raw, active: false, reuseaddr: true]
  @response """
  HTTP/1.1 204 No Content\r
  Content-Length: 0\r
  \r
  """

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def init(opts) do
    {:ok, listen_socket} = :gen_tcp.listen(opts[:port] || @port, @listen_options)

    state = %{
      caller: opts.caller,
      listen_socket: listen_socket
    }

    {:ok, state, {:continue, :accept}}
  end

  def handle_continue(:accept, state) do
    {:ok, socket} = :gen_tcp.accept(state.listen_socket)
    handle_connection(state.caller, socket)
    {:noreply, state, {:continue, :accept}}
  end

  ## Internal Functions

  @spec handle_connection(pid(), :gen_tcp.socket()) :: any()
  defp handle_connection(caller, socket) do
    with {:ok, request} <- :gen_tcp.recv(socket, 0),
         [_headers, body] <- String.split(request, "\r\n\r\n", parts: 2),
         :ok <- :gen_tcp.send(socket, @response),
         :ok <- :gen_tcp.close(socket),
         {:ok, event} <- Jason.decode(body) do
      case event["data"] do
        records when is_list(records) ->
          Enum.each(records, fn record ->
            send(caller, record)
          end)

        _ ->
          send(caller, event)
      end
    end
  end
end
