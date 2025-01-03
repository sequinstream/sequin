defmodule Sequin.ConsoleLogger do
  @moduledoc false
  @drop_metadata_keys Application.compile_env(:sequin, [__MODULE__, :drop_metadata_keys], [])
  @format_pattern Logger.Formatter.compile("[$level] [$time] $message $metadata\n")

  # Example binding. Note some keys are dropped by Elixir's default formatter:
  # [
  #   level: :info,
  #   message: "Running in passive mode, another instance is holding active mutex.",
  #   metadata: [
  #     line: 94,
  #     pid: #PID<0.942.0>,
  #     time: 1735861762188779,
  #     file: ~c"lib/sequin/mutex_owner.ex",
  #     gl: #PID<0.470.0>,
  #     domain: [:elixir],
  #     application: :sequin,
  #     mfa: {Sequin.MutexOwner, :handle_event, 4},
  #     mutex_key: "sequin:mutexed_supervisor:dev:Elixir.Sequin.Runtime.MutexedSupervisor"
  #   ],
  #   timestamp: {{2025, 1, 2}, {15, 49, 22, 188}}
  # ]
  def format(level, message, timestamp, metadata) do
    # Drop metadata keys we don't care about
    metadata = Keyword.drop(metadata, @drop_metadata_keys)

    Logger.Formatter.format(@format_pattern, level, message, timestamp, metadata)
  end
end
