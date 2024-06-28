defmodule Mix.Tasks.Merge do
  @shortdoc "Fetches PR details, checks mergeability, and merges if conditions are met."
  @moduledoc false
  use Mix.Task

  @green "\e[32m"
  @red "\e[31m"
  @blue "\e[34m"
  @reset "\e[0m"

  def run(_args) do
    owner = trimmed_cmd("gh", ~w(repo view --json owner --jq .owner.login))
    repo = trimmed_cmd("gh", ~w(repo view --json name --jq .name))
    sha = trimmed_cmd("git", ~w(rev-parse HEAD))
    local_branch_name = trimmed_cmd("git", ~w(rev-parse --abbrev-ref HEAD))

    announce("Fetching open PRs matching current branch and SHA...", @blue)
    pr_details = fetch_pr_details(owner, repo, sha, local_branch_name)

    announce("PR Details: #{inspect(pr_details)}", @blue)
    pr_count = length(pr_details)

    case pr_count do
      0 ->
        announce("No open pull request found for branch #{local_branch_name} with SHA #{sha}. Merge aborted.", @red)
        exit(:shutdown)

      1 ->
        pr_number = pr_details |> Enum.at(0) |> Map.get("number")
        mergeable_status = pr_details |> Enum.at(0) |> Map.get("mergeable")

        announce("PR Number: #{pr_number}", @blue)
        announce("Mergeable Status: #{mergeable_status}", @blue)

        if mergeable_status == "MERGEABLE" do
          check_and_merge_pr(owner, repo, sha, pr_number)
        else
          announce("Pull request ##{pr_number} is not mergeable. Merge aborted.", @red)
          exit(:shutdown)
        end

      _ ->
        announce(
          "Multiple open pull requests found for branch #{local_branch_name} with SHA #{sha}. Merge aborted.",
          @red
        )

        exit(:shutdown)
    end
  end

  defp fetch_pr_details(owner, repo, sha, branch_name) do
    {result, 0} =
      System.cmd("gh", [
        "pr",
        "list",
        "--repo",
        "#{owner}/#{repo}",
        "--json",
        "number,headRefName,headRefOid,mergeable",
        "--jq",
        "[.[] | select(.headRefOid == \"#{sha}\" and .headRefName == \"#{branch_name}\")]"
      ])

    Jason.decode!(result)
  end

  defp check_and_merge_pr(owner, repo, sha, pr_number) do
    {result, 0} =
      System.cmd("gh", [
        "api",
        "--method",
        "GET",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        "X-GitHub-Api-Version: 2022-11-28",
        "/repos/#{owner}/#{repo}/commits/#{sha}/statuses"
      ])

    statuses = Jason.decode!(result)

    if Enum.any?(statuses, fn status -> status["state"] == "success" and status["context"] == "signoff" end) do
      announce("Commit was successfully signed off. Proceeding with merge...", @green)

      {_merge_result, status} =
        System.cmd("gh", ["pr", "merge", "#{pr_number}", "--repo", "#{owner}/#{repo}", "--rebase", "--admin"],
          into: IO.stream(:stdio, :line),
          stderr_to_stdout: true
        )

      if status == 0 do
        announce("Pull request ##{pr_number} merged successfully with rebase.", @green)
      else
        announce("Failed to merge pull request ##{pr_number}.", @red)
        exit(:shutdown)
      end
    else
      announce("Commit has not been successfully signed off or signoff not found. Merge aborted.", @red)
      exit(:shutdown)
    end
  end

  defp announce(message, color) do
    IO.puts("#{color}#{message}#{@reset}")
  end

  defp trimmed_cmd(cmd, args) do
    {output, 0} = System.cmd(cmd, args)
    String.trim(output)
  end
end
