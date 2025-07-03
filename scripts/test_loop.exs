#!/usr/bin/env elixir

# Test Loop Script
# Runs `mix test` indefinitely until you press Enter
# Tracks and reports test failures with counts
#
# Features:
# - Runs tests in a loop until you press Enter
# - Tracks which tests fail and how many times
# - Shows full output for first-time failures (helpful for debugging)
# - Provides summary with failure counts at the end
#
# Usage:
#   ./scripts/test_loop.exs
#
# Press Enter to stop gracefully and see failure summary

defmodule TestLoop do
  @moduledoc """
  A script that runs `mix test` in a loop and tracks test failures.

  This is useful for detecting flaky tests by running the test suite
  multiple times and tracking which tests fail intermittently.
  """

  defstruct [
    :failures,
    :run_count,
    :start_time,
    :first_failure_outputs
  ]

  def run do
    # Ensure we're in the right directory
    unless File.exists?("mix.exs") do
      IO.puts("Error: mix.exs not found. Please run this script from the project root.")
      System.halt(1)
    end

    state = %__MODULE__{
      failures: %{},
      run_count: 0,
      start_time: DateTime.utc_now(),
      first_failure_outputs: %{}
    }

    IO.puts("Starting test loop...")
    IO.puts("Press Enter to stop gracefully and see failure summary.\n")

    # Start the input listener in a separate process
    parent = self()
    spawn_link(fn -> wait_for_input(parent) end)

    loop(state)
  end

  defp wait_for_input(parent) do
    IO.gets("")
    send(parent, :stop)
  end

  defp loop(state) do
    # Check for stop message
    receive do
      :stop ->
        IO.puts("\nStopping gracefully...")
        print_final_summary(state)
        System.halt(0)
    after
      0 -> :ok
    end

    IO.puts("=== Test Run ##{state.run_count + 1} ===")

    new_state = case run_mix_test() do
      {:ok, _output} ->
        IO.puts("✅ All tests passed")
        state

      {:error, output} ->
        IO.puts("❌ Tests failed")
        new_failures = parse_test_failures(output)
        updated_failures = merge_failures(state.failures, new_failures)

        # Show full output for first-time failures
        {updated_first_outputs, new_failure_names} = track_first_failures(state.first_failure_outputs, new_failures, output)

        if length(new_failure_names) > 0 do
          IO.puts("\n--- First-time failures (showing full output) ---")
          Enum.each(new_failure_names, fn name ->
            IO.puts("🔍 #{name}")
          end)
          IO.puts(output)
          IO.puts("--- End first-time failure output ---\n")
        end

        if map_size(new_failures) > 0 do
          IO.puts("Failures in this run:")
          print_failures(new_failures)
        end

        %{state |
          failures: updated_failures,
          first_failure_outputs: updated_first_outputs
        }
    end

    updated_state = %{new_state | run_count: new_state.run_count + 1}

    # Small delay between runs
    Process.sleep(1000)

    loop(updated_state)
  end

  defp run_mix_test do
    # Capture output for parsing failures
    {output, exit_code} = System.cmd("mix", ["test"], stderr_to_stdout: true)

    case exit_code do
      0 -> {:ok, output}
      _ -> {:error, output}
    end
  rescue
    e ->
      IO.puts("Error running mix test: #{inspect(e)}")
      {:error, "Command failed"}
  end

  defp parse_test_failures(output) when is_binary(output) do
    # Parse the output to extract failed test names
    # Look for patterns like "  1) test description (ModuleName)"
    lines = String.split(output, "\n")

    lines
    |> Enum.reduce([], fn line, acc ->
      case extract_test_name(line) do
        nil -> acc
        test_name -> [test_name | acc]
      end
    end)
    |> Enum.reduce(%{}, fn test_name, acc ->
      Map.put(acc, test_name, 1)
    end)
  end

  defp parse_test_failures(_), do: %{}

  defp extract_test_name(line) do
    # Try to extract test name from various ExUnit output formats
    cond do
      # Match "  1) test description (ModuleName)"
      match = Regex.run(~r/^\s*\d+\)\s*(.+?)\s*\((.+?)\)\s*$/, line) ->
        case match do
          [_, test_desc, module] -> "#{String.trim(test_desc)} (#{String.trim(module)})"
          _ -> nil
        end

      # Match "     test description (ModuleName)" in failure output
      match = Regex.run(~r/^\s+(.+?)\s+\((.+?)\)\s*$/, line) ->
        test_desc = String.trim(match |> Enum.at(1, ""))
        module = String.trim(match |> Enum.at(2, ""))
        if String.starts_with?(test_desc, "test ") and String.contains?(module, ".") do
          "#{test_desc} (#{module})"
        else
          nil
        end

      # Match "** (ExceptionType) message" lines that mention test names
      match = Regex.run(~r/^\s*\*\*\s*\([^)]+\).*test\s+(.+)/, line) ->
        case match do
          [_, test_desc] -> String.trim(test_desc)
          _ -> nil
        end

      # Match failure summary lines "• test description (ModuleName)"
      match = Regex.run(~r/^\s*•\s*(.+?)\s*\((.+?)\)\s*$/, line) ->
        case match do
          [_, test_desc, module] -> "#{String.trim(test_desc)} (#{String.trim(module)})"
          _ -> nil
        end

      # Match lines that start with "test " followed by description
      match = Regex.run(~r/^\s*test\s+(.+?)\s*\((.+?)\)\s*$/, line) ->
        case match do
          [_, test_desc, module] -> "test #{String.trim(test_desc)} (#{String.trim(module)})"
          _ -> nil
        end

      true -> nil
    end
  end

  defp merge_failures(existing_failures, new_failures) do
    Map.merge(existing_failures, new_failures, fn _key, old_count, new_count ->
      old_count + new_count
    end)
  end

  defp track_first_failures(existing_outputs, new_failures, output) do
    new_failure_names =
      new_failures
      |> Map.keys()
      |> Enum.filter(fn name -> not Map.has_key?(existing_outputs, name) end)

    updated_outputs =
      Enum.reduce(new_failure_names, existing_outputs, fn name, acc ->
        Map.put(acc, name, output)
      end)

    {updated_outputs, new_failure_names}
  end

  defp print_failures(failures) do
    failures
    |> Enum.sort_by(fn {_test, count} -> count end, :desc)
    |> Enum.each(fn {test_name, count} ->
      IO.puts("  • #{test_name} (#{count} time#{if count > 1, do: "s", else: ""})")
    end)
  end

  defp print_final_summary(state) do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("TEST LOOP SUMMARY")
    IO.puts(String.duplicate("=", 60))

    duration = DateTime.diff(DateTime.utc_now(), state.start_time, :second)
    IO.puts("Ran #{state.run_count} test run#{if state.run_count != 1, do: "s", else: ""} over #{format_duration(duration)}")

    if map_size(state.failures) == 0 do
      IO.puts("🎉 No test failures detected!")
    else
      IO.puts("❌ Test failures detected:")
      IO.puts("")
      print_failures(state.failures)

      total_failures = state.failures |> Map.values() |> Enum.sum()
      unique_failures = map_size(state.failures)

      IO.puts("")
      IO.puts("Total: #{total_failures} failure#{if total_failures != 1, do: "s", else: ""} across #{unique_failures} unique test#{if unique_failures != 1, do: "s", else: ""}")

      if map_size(state.first_failure_outputs) > 0 do
        IO.puts("\n--- First failure outputs (for debugging) ---")
        state.first_failure_outputs
        |> Enum.each(fn {test_name, output} ->
          IO.puts("\n🔍 #{test_name}:")
          IO.puts(String.duplicate("-", 40))
          IO.puts(output)
          IO.puts(String.duplicate("-", 40))
        end)
      end
    end

    IO.puts(String.duplicate("=", 60))
  end

  defp format_duration(seconds) when seconds < 60, do: "#{seconds}s"
  defp format_duration(seconds) when seconds < 3600 do
    minutes = div(seconds, 60)
    remaining_seconds = rem(seconds, 60)
    "#{minutes}m #{remaining_seconds}s"
  end
  defp format_duration(seconds) do
    hours = div(seconds, 3600)
    remaining_seconds = rem(seconds, 3600)
    minutes = div(remaining_seconds, 60)
    final_seconds = rem(remaining_seconds, 60)
    "#{hours}h #{minutes}m #{final_seconds}s"
  end
end

# Main execution
TestLoop.run()
