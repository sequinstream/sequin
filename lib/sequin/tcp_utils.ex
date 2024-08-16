defmodule Sequin.TcpUtils do
  @moduledoc false

  alias Sequin.Error

  require Logger

  @doc """
  Pings a host on a port to see if it is reachable.
  """
  @spec test_reachability(String.t(), number(), number()) :: :ok | {:error, Error.t()}
  def test_reachability(host, port, timeout \\ 10_000) do
    with :ok <- validate_port(port) do
      case :gen_tcp.connect(to_charlist(host), port, [], timeout) do
        {:ok, port} when is_port(port) ->
          # Succcess, we could reach host
          :gen_tcp.close(port)
          :ok

        {:error, error} ->
          Logger.error("Unable to connect to database",
            error: error,
            metadata: %{host: host, port: port}
          )

          case error do
            :nxdomain ->
              {:error, Error.validation(summary: "The host is not reachable (nxdomain).", code: :nxdomain)}

            :econnrefused ->
              {:error,
               Error.validation(summary: "The host is not reachable on that port (econnrefused).", code: :econnrefused)}

            :timeout ->
              {:error, Error.validation(summary: "Timed out attempting to reach the host on that port.", code: :timeout)}

            error ->
              Logger.error("Unknown error when attempting to connect to database",
                error: error,
                metadata: %{host: host, port: port}
              )

              {:error, Error.validation(summary: "Unknown error connecting to database: #{inspect(error)}")}
          end
      end
    end
  catch
    :exit, :badarg ->
      Logger.error("Invalid hostname for database",
        metadata: %{host: host, port: port}
      )

      {:error, Error.validation(summary: "Invalid hostname.")}
  end

  defp validate_port(port) do
    if is_integer(port) && port >= 1 && port <= 65_535 do
      :ok
    else
      {:error, Error.validation(summary: "Port must be a number between 1 and 65,535.")}
    end
  end
end
