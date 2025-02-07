defmodule Sequin.Logs do
  @moduledoc false

  alias Sequin.Logs.Log

  require Logger

  def datadog_enabled, do: Application.get_env(:sequin, :datadog)[:configured]

  def get_logs_for_consumer_message(account_id, consumer_id, trace_id) do
    if datadog_enabled() do
      get_logs_from_datadog(account_id, consumer_id, trace_id)
    else
      get_logs_from_file(account_id, consumer_id, trace_id)
    end
  end

  defp get_logs_from_file(account_id, consumer_id, trace_id) do
    log_file_path()
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.filter(&(&1 != ""))
    |> Stream.map(&Jason.decode!/1)
    |> Stream.filter(fn log ->
      log["account_id"] == account_id and log["consumer_id"] == consumer_id and log["trace_id"] == trace_id and
        log["console_logs"] == "consumer_message"
    end)
    |> Enum.map(fn log ->
      {:ok, timestamp, 0} = DateTime.from_iso8601(log["timestamp"])

      %Log{
        timestamp: timestamp,
        status: log["level"],
        account_id: log["account_id"],
        message: log["message"]
      }
    end)
    |> then(&{:ok, &1})
  end

  defp get_logs_from_datadog(account_id, consumer_id, trace_id) do
    case search_logs(
           query:
             "* @account_id:#{account_id} @consumer_id:#{consumer_id} @trace_id:#{trace_id} @console_logs:consumer_message"
         ) do
      {:ok, %{"data" => data}} ->
        logs =
          data
          |> Enum.map(&Log.from_datadog_log/1)
          |> Enum.filter(&(&1.account_id == account_id))

        {:ok, logs}

      {:ok, %{"errors" => [error | _]}} ->
        {:error, error}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def log_for_consumer_message(level \\ :info, account_id, consumer_id, trace_id, message)

  def log_for_consumer_message(level, _, _, _, _) when level not in [:info, :warning, :error] do
    raise ArgumentError, "Invalid log level: #{inspect(level)}. Valid levels are :info, :warning, :error"
  end

  def log_for_consumer_message(_, nil, _, _, _), do: raise(ArgumentError, "Invalid account_id: nil")
  def log_for_consumer_message(_, _, nil, _, _), do: raise(ArgumentError, "Invalid consumer_id: nil")
  def log_for_consumer_message(_, _, _, nil, _), do: raise(ArgumentError, "Invalid trace_id: nil")
  def log_for_consumer_message(_, _, _, _, nil), do: raise(ArgumentError, "Invalid message: nil")

  def log_for_consumer_message(level, account_id, consumer_id, trace_ids, message) when is_list(trace_ids) do
    if datadog_enabled() do
      Enum.each(trace_ids, &log_to_logger(level, account_id, consumer_id, &1, message))
    else
      log_to_file(level, account_id, consumer_id, trace_ids, message)
    end
  end

  def log_for_consumer_message(level, account_id, consumer_id, trace_id, message) do
    if datadog_enabled() do
      log_to_logger(level, account_id, consumer_id, trace_id, message)
    else
      log_to_file(level, account_id, consumer_id, trace_id, message)
    end
  end

  defp log_to_file(level, account_id, consumer_id, trace_ids, message) when is_list(trace_ids) do
    entries =
      trace_ids
      |> Enum.map(&log_entry(level, account_id, consumer_id, &1, message))
      |> Enum.map_join("\n", &Jason.encode!/1)

    File.mkdir_p!(Path.dirname(log_file_path()))
    File.write!(log_file_path(), entries, [:append])
  end

  defp log_to_file(level, account_id, consumer_id, trace_id, message) do
    log_entry = %{
      timestamp: DateTime.to_iso8601(DateTime.utc_now()),
      level: to_string(level),
      account_id: account_id,
      consumer_id: consumer_id,
      trace_id: trace_id,
      message: message,
      console_logs: "consumer_message"
    }

    log_line = Jason.encode!(log_entry) <> "\n"
    File.mkdir_p!(Path.dirname(log_file_path()))
    File.write!(log_file_path(), log_line, [:append])
  end

  defp log_entry(level, account_id, consumer_id, trace_id, message) do
    %{
      timestamp: DateTime.to_iso8601(DateTime.utc_now()),
      level: to_string(level),
      account_id: account_id,
      consumer_id: consumer_id,
      trace_id: trace_id,
      message: message,
      console_logs: "consumer_message"
    }
  end

  defp log_file_path do
    Path.join(:code.priv_dir(:sequin), "logs/consumer_messages.log")
  end

  defp log_to_logger(level, account_id, consumer_id, trace_id, message) do
    metadata = [account_id: account_id, consumer_id: consumer_id, trace_id: trace_id, console_logs: :consumer_message]

    case level do
      :info -> Logger.info(message, metadata)
      :warning -> Logger.warning(message, metadata)
      :error -> Logger.error(message, metadata)
    end
  end

  defp search_logs(params) do
    if datadog_enabled() do
      search_logs_in_datadog(params)
    else
      raise "Log search not implemented for file-based logs. See Logs.get_logs_from_file/2."
    end
  end

  defp search_logs_in_datadog(params) do
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

  defp api_key, do: Application.fetch_env!(:sequin, :datadog)[:api_key]
  defp app_key, do: Application.fetch_env!(:sequin, :datadog)[:app_key]

  defp default_query do
    Application.fetch_env!(:sequin, :datadog)[:default_query] || ""
  end

  def trim_log_file do
    log_file = log_file_path()

    case File.write(log_file, "", [:write]) do
      :ok ->
        Logger.info("Cleared consumer messages log file: #{log_file}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to clear consumer messages log file: #{log_file}, reason: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
