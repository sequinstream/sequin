defmodule Sequin.Logs.Log do
  @moduledoc false
  alias __MODULE__

  @derive Jason.Encoder
  defstruct [
    :account_id,
    :timestamp,
    :message,
    :status
  ]

  def from_datadog_log(log) do
    attributes = get_in(log, ["attributes", "attributes"])

    %Log{
      account_id: get_in(attributes, ["account_id"]),
      timestamp:
        attributes
        |> get_in(["syslog", "timestamp"])
        |> DateTime.from_iso8601()
        |> case do
          {:ok, timestamp, 0} -> timestamp
          {:error, _reason} -> nil
        end,
      message: get_in(log, ["attributes", "message"]),
      status: get_in(log, ["attributes", "status"])
    }
  end
end
