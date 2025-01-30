defmodule Sequin.System do
  @moduledoc false

  @doc """
  Get the total memory of the system in bytes.

  Only works on Linux (our Docker images use Debian).
  """
  def total_memory_bytes do
    case File.read("/proc/meminfo") do
      {:ok, contents} ->
        contents
        |> String.split("\n")
        |> Enum.find(fn line -> String.starts_with?(line, "MemTotal:") end)
        |> parse_memory_line()
        |> case do
          nil -> {:error, :parse_failed}
          bytes -> {:ok, bytes}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_memory_line(line) when is_binary(line) do
    case Regex.run(~r/MemTotal:\s+(\d+)\s+kB/, line) do
      [_, kb_str] ->
        kb_str |> String.to_integer() |> Kernel.*(1024)

      _ ->
        nil
    end
  end

  defp parse_memory_line(_), do: nil
end
