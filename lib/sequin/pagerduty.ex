defmodule Sequin.Pagerduty do
  @moduledoc """
  Create or resolve alerts in PagerDuty.
  """
  require Logger

  @events_api_url "https://events.pagerduty.com/v2/enqueue"

  @type dedup_key :: String.t()
  @type link :: %{
          href: String.t(),
          text: String.t()
        }
  @type severity :: :critical | :error | :info | :warning

  @summary_limit 1024

  @spec alert(dedup_key(), String.t()) :: :ok | {:error, String.t()}
  @spec alert(dedup_key(), String.t(), [opt]) :: :ok | {:error, String.t()}
        when opt: {:links, [link()]} | {:severity, severity()} | {:source, String.t()} | {:req_opts, keyword()}
  def alert(key, summary, opts \\ []) do
    json = build_payload(:trigger, key, summary, opts)

    if enabled?() do
      send_event(json, opts)
    else
      :ok
    end
  end

  @spec resolve(dedup_key(), String.t(), [opt]) :: :ok | {:error, String.t()}
        when opt: {:links, [link()]} | {:severity, severity()} | {:source, String.t()} | {:req_opts, keyword()}
  def resolve(key, summary, opts \\ []) do
    json = build_payload(:resolve, key, summary, opts)

    if enabled?() do
      send_event(json, opts)
    else
      :ok
    end
  end

  defp build_payload(action, key, summary, opts) do
    %{
      "routing_key" => config()[:integration_key],
      "dedup_key" => key,
      "links" => Keyword.get(opts, :links, []),
      "event_action" => action,
      "payload" => %{
        "summary" => String.slice(summary, 0..(@summary_limit - 1)),
        "source" => Keyword.get(opts, :source, Application.get_env(:sequin_web, :console_url)),
        "severity" => Keyword.get(opts, :severity, :warning),
        "timestamp" => DateTime.to_iso8601(DateTime.utc_now())
      }
    }
  end

  defp send_event(payload, opts) do
    default_req_opts = Keyword.get(config(), :req_opts, [])
    req_opts = Keyword.get(opts, :req_opts, [])
    req_opts = Keyword.merge(default_req_opts, req_opts)

    [
      method: :post,
      url: @events_api_url,
      json: payload
    ]
    |> Req.new()
    |> Req.merge(req_opts)
    |> Req.post()
    |> case do
      {:ok, %{status: 202}} -> :ok
      {:ok, response} -> {:error, "Unexpected response: #{inspect(response)}"}
      {:error, error} -> {:error, "Request failed: #{inspect(error)}"}
    end
  end

  defp config do
    Application.get_env(:sequin, Sequin.Pagerduty, [])
  end

  defp enabled? do
    not is_nil(config()[:integration_key])
  end
end
