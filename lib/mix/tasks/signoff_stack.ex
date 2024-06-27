defmodule Mix.Tasks.Signoff.Stack do
  @shortdoc "Signs off on all PRs in a Graphite Stack"
  @moduledoc """
  This task signs off on all PRs in a Graphite Stack.

  It starts from the bottom of the stack, runs the signoff task,
  and then moves up the stack until it reaches the top or encounters a failure.
  """
  use Mix.Task

  @green "\e[32m"
  @red "\e[31m"
  @blue "\e[34m"
  @reset "\e[0m"

  def run(_args) do
    IO.puts("#{@blue}Starting signoff process for the Graphite Stack#{@reset}")

    # Move to the bottom of the stack
    System.cmd("gt", ["bottom"])

    signoff_stack()
  end

  defp signoff_stack do
    case run_signoff() do
      :ok ->
        case move_up_stack() do
          :top -> IO.puts("#{@green}Successfully signed off on all PRs in the stack#{@reset}")
          :continue -> signoff_stack()
        end

      :error ->
        IO.puts("#{@red}Signoff failed. Stopping the process.#{@reset}")
        System.halt(1)
    end
  end

  defp run_signoff do
    IO.puts("#{@blue}Running signoff for the current PR#{@reset}")

    Mix.Task.reenable("signoff")

    case Mix.Task.run("signoff") do
      :ok ->
        IO.puts("#{@green}Signoff successful for the current PR#{@reset}")
        :ok

      _ ->
        IO.puts("#{@red}Signoff failed for the current PR#{@reset}")
        :error
    end
  end

  defp move_up_stack do
    {output, _status} = System.cmd("gt", ["up"])

    if String.contains?(output, "Already at the top most branch in the stack") do
      :top
    else
      :continue
    end
  end
end
