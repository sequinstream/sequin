defmodule Sequin.Logs.RotateLogsWorker do
  @moduledoc false
  use Oban.Worker

  alias Sequin.Logs

  require Logger

  @impl Oban.Worker
  def perform(_) do
    Logger.info("[RotateLogsWorker] Running")

    if Logs.datadog_enabled() do
      Logger.info("[RotateLogsWorker] Datadog is enabled, skipping")
      :ok
    else
      Logger.info("[RotateLogsWorker] Trimming log file")
      Logs.trim_log_file()
    end
  end
end
