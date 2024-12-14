defmodule Sequin.NetworkUtils do
  @moduledoc false

  alias Sequin.Error

  require Logger

  @spec check_ipv6(String.t()) :: {:ok, true | false} | {:error, Error.t()}
  def check_ipv6(host) do
    with {{:error, :nxdomain}, :inet} <- {:inet.getaddr(to_charlist(host), :inet), :inet},
         {{:error, :nxdomain}, :inet6} <- {:inet.getaddr(to_charlist(host), :inet6), :inet6} do
      {:error, Error.validation(summary: "The host is not reachable (nxdomain).", code: :nxdomain)}
    else
      {{:ok, _}, :inet} ->
        {:ok, false}

      {{:ok, _}, :inet6} ->
        {:ok, true}
    end
  end

  @doc """
  Pings a host on a port to see if it is reachable.
  """
  @spec test_tcp_reachability(String.t(), number(), boolean(), number()) :: :ok | {:error, Error.t()}
  def test_tcp_reachability(host, port, ipv6, timeout \\ 10_000) do
    with :ok <- validate_port(port) do
      case :gen_tcp.connect(to_charlist(host), port, ipv6_opts(ipv6), timeout) do
        {:ok, port} when is_port(port) ->
          # Succcess, we could reach host
          :gen_tcp.close(port)
          :ok

        {:error, error} ->
          case error do
            :nxdomain ->
              {:error, Error.validation(summary: "The host is not reachable (nxdomain).", code: :nxdomain)}

            :econnrefused ->
              {:error,
               Error.validation(summary: "The host is not reachable on that port (econnrefused).", code: :econnrefused)}

            :timeout ->
              {:error, Error.validation(summary: "Timed out attempting to reach the host on that port.", code: :timeout)}

            error ->
              {:error, Error.validation(summary: "Unknown error connecting to host: #{inspect(error)}")}
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

  defp ipv6_opts(false), do: []
  defp ipv6_opts(true), do: [:inet6]
end
