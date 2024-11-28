defmodule Sequin.Posthog do
  @moduledoc """
  A client for interacting with Posthog analytics.
  """

  @doc """
  Captures a single event.
  """
  def capture(event, properties, opts \\ [])

  def capture(event, properties, opts) when is_list(opts) do
    body = build_event(event, properties, Keyword.get(opts, :timestamp))

    opts =
      opts
      |> with_defaults()
      |> Keyword.update!(:req_opts, &Keyword.put(&1, :url, "/capture"))

    if Keyword.get(opts, :async, true) do
      async_post(body, opts)
    else
      post(body, opts)
    end
  end

  def capture(event, properties, timestamp) when is_binary(event) or is_atom(event) do
    capture(event, properties, timestamp: timestamp)
  end

  @doc """
  Captures a batch of events.
  """
  def batch(events, opts \\ []) do
    body = %{
      batch:
        Enum.map(events, fn {event, properties, timestamp} ->
          build_event(event, properties, timestamp)
        end)
    }

    opts =
      opts
      |> with_defaults()
      |> Keyword.update!(:req_opts, &Keyword.put(&1, :url, "/capture"))

    if Keyword.get(opts, :async, true) do
      async_post(body, opts)
    else
      post(body, opts)
    end
  end

  defp build_event(event, properties, timestamp) do
    {distinct_id, other_properties} = Map.pop(properties, :distinct_id)

    # If self-hosted, remove PII from properties
    properties =
      if Application.get_env(:sequin, :self_hosted, false) do
        Map.delete(other_properties.properties, :email)
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

  defp base_req(body, opts) do
    body = Map.put(body, :api_key, Keyword.get(opts, :api_key))

    [json: body, method: :post]
    |> Req.new()
    |> Req.merge(opts[:req_opts])
  end

  defp post(body, opts) do
    unless is_disabled(opts) do
      body
      |> base_req(opts)
      |> Req.run()
    end
  end

  defp async_post(body, opts) do
    unless is_disabled(opts) do
      Task.Supervisor.start_child(Sequin.TaskSupervisor, fn ->
        body
        |> base_req(opts)
        |> Req.run()
      end)
    end
  end

  defp is_disabled(opts) do
    cond do
      Keyword.get(opts, :is_disabled, false) -> true
      is_nil(Keyword.get(opts, :api_key)) -> true
      true -> false
    end
  end

  defp with_defaults(opts) do
    {default_req_opts, default_opts} = Keyword.pop(config(), :req_opts, [])
    {req_opts, opts} = Keyword.pop(opts, :req_opts, [])

    default_opts
    |> Keyword.merge(opts)
    |> Keyword.put(:req_opts, Keyword.merge(default_req_opts, req_opts))
  end

  defp config do
    Application.get_env(:sequin, Sequin.Posthog, [])
  end
end
