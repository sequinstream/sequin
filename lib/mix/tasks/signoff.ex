defmodule Mix.Tasks.Signoff do
  @shortdoc "Signs off on the current repository state by running various checks and updating GitHub."
  @moduledoc false
  use Mix.Task

  # ANSI color codes
  @green "\e[32m"
  @red "\e[31m"
  @blue "\e[34m"
  @yellow "\e[33m"
  # Resets the color to default
  @reset "\e[0m"

  def run(_args) do
    start_time = System.monotonic_time(:second)

    # Ensure the repository is clean
    if clean_repository?() do
      owner = trimmed_cmd("gh", ~w(repo view --json owner --jq .owner.login))
      repo = trimmed_cmd("gh", ~w(repo view --json name --jq .name))
      sha = trimmed_cmd("git", ~w(rev-parse HEAD))
      user = trimmed_cmd("git", ~w(config user.name))

      if already_signed_off?(owner, repo, sha) do
        IO.puts("#{@yellow}Commit #{sha} has already been signed off. No action needed.#{@reset}")
        :ok
      else
        IO.puts("#{@blue}Attempting to sign off on #{sha} in #{owner}/#{repo} as #{user}#{@reset}")
        perform_signoff(owner, repo, sha, user, start_time)
      end
    else
      IO.puts(:stderr, "#{@red}Can't sign off on a dirty repository!#{@reset}")
      System.cmd("git", ["status"])
      :error
    end
  end

  defp already_signed_off?(owner, repo, sha) do
    user = trimmed_cmd("git", ~w(config user.name))

    {output, 0} =
      System.cmd(
        "gh",
        [
          "api",
          "--method",
          "GET",
          "-H",
          "Accept: application/vnd.github+json",
          "-H",
          "X-GitHub-Api-Version: 2022-11-28",
          "/repos/#{owner}/#{repo}/commits/#{sha}/statuses"
        ],
        stderr_to_stdout: true
      )

    statuses = Jason.decode!(output)

    Enum.any?(statuses, fn status ->
      status["context"] == "signoff" &&
        status["state"] == "success" &&
        String.starts_with?(status["description"], "Signed off by #{user}")
    end)
  end

  defp perform_signoff(owner, repo, sha, user, start_time) do
    Enum.each(
      [
        {"mix format --check-formatted", []},
        {"mix compile --warnings-as-errors", [env: [{"MIX_ENV", "prod"}]]},
        {"mix test", []}
      ],
      &run_step/1
    )

    # [
    #   {"mix credo", []},
    #   {"mix dialyzer", []}
    # ]
    # |> Enum.map(fn {cmd, args} -> Task.async(fn -> run_step({cmd, args}) end) end)
    # |> Task.await_many(:infinity)

    # Report successful sign off to GitHub
    description = "Signed off by #{user} (#{System.monotonic_time(:second) - start_time} seconds)"
    report_success(owner, repo, sha, description)
    IO.puts("#{@green}Signed off on #{sha} in #{System.monotonic_time(:second) - start_time} seconds#{@reset}")
    :ok
  end

  defp clean_repository? do
    {output, 0} = System.cmd("git", ~w(status --porcelain))
    output == ""
  end

  defp run_step({cmd, system_args}) do
    system_args =
      system_args
      |> Keyword.put_new(:stderr_to_stdout, true)
      |> Keyword.put_new_lazy(:into, fn -> IO.stream(:stdio, :line) end)

    start_time = System.monotonic_time(:second)
    IO.puts("#{@blue}Run #{cmd}#{@reset}")
    # Get color
    cmd_args = ["--erl", "-elixir ansi_enabled true", "-S"] ++ String.split(cmd, " ")
    res = System.cmd("elixir", cmd_args, system_args)

    case res do
      {output, 0} ->
        IO.puts("#{@green}Completed #{cmd} in #{System.monotonic_time(:second) - start_time} seconds#{@reset}")
        output

      {_, _} ->
        IO.puts(:stderr, "#{@red}Failed to run #{cmd}#{@reset}")
        Process.exit(self(), :shutdown)
    end
  end

  defp trimmed_cmd(cmd, args) do
    {output, 0} = System.cmd(cmd, args)
    String.trim(output)
  end

  defp report_success(owner, repo, sha, description) do
    res =
      System.cmd(
        "gh",
        [
          "api",
          "--method",
          "POST",
          "--silent",
          "-H",
          "Accept: application/vnd.github+json",
          "-H",
          "X-GitHub-Api-Version: 2022-11-28",
          "/repos/#{owner}/#{repo}/statuses/#{sha}",
          "-f",
          "context=signoff",
          "-f",
          "state=success",
          "-f",
          "description=#{description}"
        ],
        into: IO.stream(:stdio, :line),
        stderr_to_stdout: true
      )

    case res do
      {_, 0} ->
        IO.puts("#{@green}Reported success to GitHub#{@reset}")

      {_, _} ->
        IO.puts(:stderr, "#{@red}Failed to report success to GitHub#{@reset}")
        Process.exit(self(), :shutdown)
    end
  end
end
