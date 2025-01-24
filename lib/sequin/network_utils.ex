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

  @doc """
  Measures average TCP latency to a given endpoint.
  Returns the average latency in milliseconds.

  Options:
  - :timeout - Connection timeout in milliseconds (default: 5000)
  - :samples - Number of measurements to take (default: 3)
  """
  @spec measure_latency(String.t(), number(), keyword()) :: {:ok, float()} | {:error, Error.t()}
  def measure_latency(host, port, opts \\ []) do
    with :ok <- validate_port(port) do
      timeout = Keyword.get(opts, :timeout, 5000)
      samples = Keyword.get(opts, :samples, 3)

      measurements =
        1..samples
        |> Enum.map(fn _ -> measure_single_latency(host, port, timeout) end)
        |> Enum.reject(&is_nil/1)

      case measurements do
        [] ->
          {:error, Error.validation(summary: "Failed to connect to #{host}:#{port}")}

        measurements ->
          {:ok, Enum.sum(measurements) / length(measurements)}
      end
    end
  end

  defp measure_single_latency(host, port, timeout) do
    start_time = System.monotonic_time(:millisecond)

    with {:ok, is_ipv6} <- check_ipv6(host),
         {:ok, socket} <-
           :gen_tcp.connect(to_charlist(host), port, [:binary, active: false] ++ ipv6_opts(is_ipv6), timeout) do
      end_time = System.monotonic_time(:millisecond)
      :gen_tcp.close(socket)
      end_time - start_time
    else
      {:error, _reason} -> nil
    end
  end
end
