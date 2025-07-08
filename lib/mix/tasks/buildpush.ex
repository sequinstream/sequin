defmodule Mix.Tasks.Buildpush do
  @shortdoc "Builds and pushes the Docker image after verifying conditions"
  @moduledoc false
  # credo:disable-for-this-file
  use Mix.Task

  @green "\e[32m"
  @red "\e[31m"
  @blue "\e[34m"
  @reset "\e[0m"
  @yellow "\e[33m"

  def run(args) do
    Application.ensure_all_started(:telemetry)
    Application.ensure_all_started(:req)
    start_time = System.monotonic_time(:second)

    Mix.Task.run("app.config")

    config = load_config()

    owner = trimmed_cmd("gh", ~w(repo view --json owner --jq .owner.login))
    repo = trimmed_cmd("gh", ~w(repo view --json name --jq .name))
    sha = trimmed_cmd("git", ~w(rev-parse HEAD))
    branch = trimmed_cmd("git", ~w(rev-parse --abbrev-ref HEAD))

    with :ok <- verify_clean_workdir(args),
         :ok <- verify_branch(branch),
         :ok <- verify_signoff(owner, repo, sha) do
      build_and_push(start_time, owner, repo, sha, config)
    else
      {:error, message} ->
        exit_with_duration(start_time, message)
    end
  end

  defp load_config do
    case Application.fetch_env!(:sequin, :buildpush) do
      nil ->
        IO.puts(:stderr, """
        #{@red}Error: Configuration for :buildpush not found.
        Please ensure you have the following in your config/dev.secret.exs:

        config :ix,
          buildpush: [
            aws_region: "us-east-1",
            aws_access_key_id: "secret",
            aws_secret_access_key: "secret",
            dockerhub_username: "sequin",
            dockerhub_token: "secret",
            ecr_url: "{accnt-id}.dkr.ecr.us-east-1.amazonaws.com",
            image_name: "sequin",
            slack_bots_webhook_url: "someurl",
            buddy_webhook_token_main: "secret",
            sentry_dsn: "https://sentry.io/some-dsn"
          ]
        #{@reset}
        """)

        exit(:shutdown)

      config ->
        config
    end
  end

  defp verify_branch(branch) do
    if branch == "main" do
      IO.puts("#{@green}On main branch. Proceeding...#{@reset}")
      :ok
    else
      {:error, "Not on main branch. Current branch: #{branch}"}
    end
  end

  defp verify_signoff(owner, repo, sha) do
    case System.cmd("gh", [
           "api",
           "--method",
           "GET",
           "-H",
           "Accept: application/vnd.github+json",
           "-H",
           "X-GitHub-Api-Version: 2022-11-28",
           "/repos/#{owner}/#{repo}/commits/#{sha}/statuses"
         ]) do
      {result, 0} ->
        statuses = Jason.decode!(result)

        if Enum.any?(statuses, fn status -> status["state"] == "success" and status["context"] == "signoff" end) do
          IO.puts("#{@green}Commit has been signed off. Proceeding...#{@reset}")
          :ok
        else
          {:error, "Commit has not been successfully signed off. Run `mix signoff` first."}
        end

      {_output, _} ->
        IO.puts(:stderr, "Error fetching commit statuses")
        exit(:shutdown)
    end
  end

  defp build_and_push(start_time, owner, repo, sha, config) do
    IO.puts("#{@blue}Building and pushing Docker image...#{@reset}")

    if !config[:sentry_dsn] do
      raise "config[:sentry_dsn] was not set, please update your config/dev.secret.exs"
    end

    env = [
      {"AWS_REGION", config[:aws_region]},
      {"AWS_ACCESS_KEY_ID", config[:aws_access_key_id]},
      {"AWS_SECRET_ACCESS_KEY", config[:aws_secret_access_key]},
      {"DOCKERHUB_USERNAME", config[:dockerhub_username]},
      {"DOCKERHUB_TOKEN", config[:dockerhub_token]}
    ]

    build_args = [
      "MIGRATOR=Sequin.Release",
      "RELEASE_NAME=sequin",
      "RELEASE_VERSION=#{sha}",
      "SENTRY_DSN=#{config[:sentry_dsn]}"
    ]

    # Login to Docker Hub
    res =
      System.cmd(
        "sh",
        ["-c", "echo #{config[:dockerhub_token]} | docker login -u #{config[:dockerhub_username]} --password-stdin"],
        env: env
      )

    case res do
      {_, 0} ->
        IO.puts("#{@green}Successfully logged in to Docker Hub#{@reset}")

      {_output, _} ->
        exit_with_duration(start_time, "Error logging in to Docker Hub")
    end

    # Login to Amazon ECR
    res =
      :os.cmd(
        ~c"aws ecr get-login-password --region #{config[:aws_region]} | docker login --username AWS --password-stdin #{config[:ecr_url]}"
      )

    if String.contains?(to_string(res), "Login Succeeded") do
      IO.puts("#{@green}Successfully logged in to Amazon ECR#{@reset}")
    else
      exit_with_duration(start_time, "Error logging in to Amazon ECR: #{res}")
    end

    cmd =
      [
        "buildx",
        "build",
        "--push",
        "--tag",
        "#{config[:ecr_url]}/#{config[:image_name]}:#{sha}",
        "--tag",
        "#{config[:ecr_url]}/#{config[:image_name]}:latest",
        "--cache-from",
        "type=registry,ref=#{config[:ecr_url]}/#{config[:image_name]}:cache",
        "--cache-to",
        "type=registry,image-manifest=true,oci-mediatypes=true,ref=#{config[:ecr_url]}/#{config[:image_name]}:cache,mode=max",
        "--provenance=false"
      ] ++ Enum.flat_map(build_args, fn arg -> ["--build-arg", arg] end) ++ ["."]

    res = System.cmd("docker", cmd, env: env, into: IO.stream(:stdio, :line), stderr_to_stdout: true)

    # Build and push the Docker image
    case res do
      {_, 0} ->
        IO.puts("#{@green}Successfully built and pushed Docker image#{@reset}")

      {_output, _} ->
        exit_with_duration(start_time, "Error building and pushing Docker image")
    end

    # Add status check to GitHub
    report_status(start_time, owner, repo, sha, "Build and push completed successfully")

    IO.puts("#{@green}Build and push completed successfully#{@reset}")
  end

  # defp post_deploy_button_to_slack(start_time, sha, config) do
  #   commit_message = trimmed_cmd("git", ~w(log -1 --pretty=format:%s))
  #   commit_message_escaped = escape_string(commit_message)
  #   commit_author = trimmed_cmd("git", ~w(log -1 --pretty=format:%an))
  #   ref = trimmed_cmd("git", ~w(rev-parse --abbrev-ref HEAD))

  #   slack_payload = %{
  #     blocks: [
  #       %{
  #         type: "section",
  #         text: %{
  #           type: "mrkdwn",
  #           text: "New image available for deploy (`#{sha}`)."
  #         }
  #       },
  #       %{
  #         type: "actions",
  #         elements: [
  #           %{
  #             type: "button",
  #             text: %{
  #               type: "plain_text",
  #               text: "Deploy Sequin"
  #             },
  #             url: "#{config[:buddy_webhook_url]}&revision=#{sha}",
  #             style: "primary",
  #             confirm: %{
  #               title: %{
  #                 type: "plain_text",
  #                 text: "Please confirm"
  #               },
  #               confirm: %{
  #                 type: "plain_text",
  #                 text: "Deploy Sequin"
  #               },
  #               deny: %{
  #                 type: "plain_text",
  #                 text: "Cancel"
  #               }
  #             }
  #           }
  #         ]
  #       },
  #       %{
  #         type: "context",
  #         elements: [
  #           %{
  #             type: "mrkdwn",
  #             text: "Commit: *#{commit_message_escaped}*"
  #           },
  #           %{
  #             type: "mrkdwn",
  #             text: "Author: *#{commit_author}*"
  #           },
  #           %{
  #             type: "mrkdwn",
  #             text: "Branch: *#{ref}*"
  #           }
  #         ]
  #       }
  #     ]
  #   }

  #   case Req.post(url: config[:slack_bots_webhook_url], json: slack_payload) do
  #     {:ok, %Req.Response{status: 200}} ->
  #       IO.puts("#{@green}Successfully posted deploy button to Slack#{@reset}")

  #     {:error, reason} ->
  #       exit_with_duration(start_time, "Error posting deploy button to Slack: #{inspect(reason)}")
  #   end
  # end

  # defp escape_string(str) do
  #   str
  #   |> String.replace("\\", "\\\\")
  #   |> String.replace("\"", "\\\"")
  #   |> String.replace("\n", "\\n")
  #   |> String.replace("\r", "\\r")
  #   |> String.replace("\t", "\\t")
  # end

  defp report_status(start_time, owner, repo, sha, description) do
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
          "context=build-and-push",
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
        IO.puts("#{@green}Successfully reported status to GitHub#{@reset}")

      {_, _} ->
        exit_with_duration(start_time, "Error reporting status to GitHub")
    end
  end

  defp trimmed_cmd(cmd, args) do
    case System.cmd(cmd, args) do
      {output, 0} ->
        String.trim(output)

      {_, _} ->
        IO.puts(:stderr, "Error executing command '#{cmd} #{Enum.join(args, " ")}'")
        exit(:shutdown)
    end
  end

  defp format_duration(seconds) do
    minutes = div(seconds, 60)
    remaining_seconds = rem(seconds, 60)
    "#{minutes}m#{remaining_seconds}s"
  end

  defp exit_with_duration(start_time, error) do
    end_time = System.monotonic_time(:second)
    duration = format_duration(end_time - start_time)
    IO.puts(:stderr, "#{@red}#{error}")
    IO.puts(:stderr, "#{@red}Exited after #{duration}#{@reset}")
    exit(:shutdown)
  end

  defp verify_clean_workdir(args) do
    if "--dirty" in args do
      IO.puts("#{@yellow}Warning: Running buildpush on a dirty repository.#{@reset}")
      IO.puts("#{@yellow}Current git status:#{@reset}")
      System.cmd("git", ["status", "--short"], into: IO.stream(:stdio, :line))
      IO.puts("")
      :ok
    else
      case System.cmd("git", ["status", "--porcelain"]) do
        {"", 0} ->
          :ok

        {_, 0} ->
          {:error, "Repository is dirty. Use 'mix buildpush --dirty' to override."}

        _ ->
          {:error, "Failed to check repository status"}
      end
    end
  end
end
