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
    {distinct_id, other_properties} = Map.pop(properties, :distinct_id)

    # If self-hosted, remove PII from properties
    properties =
      if Application.get_env(:sequin, :self_hosted, false) do
        Map.delete(other_properties.properties, "email")
      else
        other_properties.properties
      end

    %{
      event: to_string(event),
      distinct_id: distinct_id,
      properties: properties,
      timestamp: timestamp
    }
  end

  defp async_post(path, body, config) do
    unless is_disabled() do
      url = config |> Keyword.get(:api_url) |> URI.merge(path) |> URI.to_string()
      body = Map.put(body, :api_key, Keyword.get(config, :api_key))

      Task.Supervisor.start_child(Sequin.TaskSupervisor, fn ->
        Req.post(url, json: body)
      end)
    end
  end

  defp is_disabled do
    case config() do
      {:ok, config} -> Keyword.get(config, :is_disabled, false)
      {:error, :not_configured} -> false
    end
  end

  defp config do
    case Application.fetch_env(:sequin, Sequin.Posthog) do
      {:ok, config} -> {:ok, config}
      :error -> {:error, :not_configured}
    end
  end
end
