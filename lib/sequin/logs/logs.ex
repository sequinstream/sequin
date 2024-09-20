defmodule Sequin.Logs do
  @moduledoc false

  alias Sequin.Logs.Log

  require Logger

  def get_logs_for_consumer_message(account_id, trace_id) do
    case search_logs(query: "* @account_id:#{account_id} @trace_id:#{trace_id} @console_logs:consumer_message") do
      {:ok, %{"data" => data}} ->
        logs =
          data
          |> Enum.map(&Log.from_datadog_log/1)
          |> Enum.filter(&(&1.account_id == account_id))

        {:ok, logs}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def log_for_consumer_message(level \\ :info, account_id, trace_id, message)

  def log_for_consumer_message(level, _, _, _) when level not in [:info, :warning, :error] do
    raise ArgumentError, "Invalid log level: #{inspect(level)}. Valid levels are :info, :warning, :error"
  end

  def log_for_consumer_message(_, nil, _, _), do: raise(ArgumentError, "Invalid account_id: nil")
  def log_for_consumer_message(_, _, nil, _), do: raise(ArgumentError, "Invalid trace_id: nil")
  def log_for_consumer_message(_, _, _, nil), do: raise(ArgumentError, "Invalid message: nil")

  def log_for_consumer_message(level, account_id, trace_id, message) do
    metadata = [account_id: account_id, trace_id: trace_id, console_logs: :consumer_message]

    case level do
      :info -> Logger.info(message, metadata)
      :warning -> Logger.warning(message, metadata)
      :error -> Logger.error(message, metadata)
    end
  end

  def search_logs(params \\ []) do
    query = Keyword.get(params, :query, "*")

    # Construct the API request body
    body = %{
      filter: %{
        query: query <> " " <> default_query(),
        from: "now-1h",
        to: "now"
      }
    }

    # Make the API request to Datadog
    case make_api_request(body) do
      {:ok, response} -> {:ok, parse_response(response)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp make_api_request(body) do
    Req.post("https://api.datadoghq.com/api/v2/logs/events/search",
      json: body,
      headers: [
        {"DD-API-KEY", api_key()},
        {"DD-APPLICATION-KEY", app_key()}
      ]
    )
  end

  defp parse_response(response) do
    response.body
  end

  # defp api_key, do: Application.fetch_env!(:sequin, :datadog)[:api_key]
  # defp app_key, do: Application.fetch_env!(:sequin, :datadog)[:app_key]
  defp api_key, do: "9b0b1923c0583dcf52f06f6c20576555"
  defp app_key, do: "0c462828725f0cf3176579c3252d9fb98d4e139d"

  # defp default_query do
  #   Application.fetch_env!(:sequin, :datadog)[:default_query] || ""
  # end
  defp default_query, do: ""
end
