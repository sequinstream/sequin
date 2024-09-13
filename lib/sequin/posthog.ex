defmodule Sequin.Posthog do
  @moduledoc """
  A client for interacting with Posthog analytics.
  """

  @doc """
  Captures a single event.
  """
  def capture(event, properties, opts \\ [])

  def capture(event, properties, opts) when is_list(opts) do
    with {:ok, config} <- config() do
      body = build_event(event, properties, Keyword.get(opts, :timestamp))
      async_post("/capture", body, config)
    end
  end

  def capture(event, properties, timestamp) when is_binary(event) or is_atom(event) do
    capture(event, properties, timestamp: timestamp)
  end

  @doc """
  Captures a batch of events.
  """
  def batch(events, _opts \\ []) do
    with {:ok, config} <- config() do
      body = %{
        batch:
          Enum.map(events, fn {event, properties, timestamp} ->
            build_event(event, properties, timestamp)
          end)
      }

      async_post("/capture", body, config)
    end
  end

  defp build_event(event, properties, timestamp) do
    %{
      event: to_string(event),
      properties: Map.new(properties),
      timestamp: timestamp
    }
  end

  defp async_post(path, body, config) do
    url = config |> Keyword.get(:api_url) |> URI.merge(path) |> URI.to_string()
    body = Map.put(body, :api_key, Keyword.get(config, :api_key))

    Task.Supervisor.start_child(Sequin.TaskSupervisor, fn ->
      Req.post(url, json: body)
    end)
  end

  defp config do
    case Application.fetch_env(:sequin, Sequin.Posthog) do
      {:ok, config} -> {:ok, config}
      :error -> {:error, :not_configured}
    end
  end
end
