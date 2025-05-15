defmodule Sequin.CheckSystemHealthWorker do
  @moduledoc false
  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1

  alias Sequin.CheckSystemHealth
  alias Sequin.Error.ServiceError

  require Logger

  @impl Oban.Worker
  def perform(_job) do
    case CheckSystemHealth.check() do
      :ok ->
        :ok

      {:error, %ServiceError{} = error} ->
        if error.details do
          Logger.error("[System Health Check] Error with service: #{error.message} (details=#{inspect(error.details)})",
            details: error.details
          )
        else
          Logger.error("[System Health Check] Error with service: #{error.message}")
        end

        {:error, error}
    end
  end

  def enqueue do
    Oban.insert(new(%{}))
  end
end
