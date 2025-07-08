defmodule Sequin.Telemetry.PosthogReporter do
  @moduledoc false
  use GenServer, shutdown: 2_000

  require Logger

  @default_publish_interval to_timeout(minute: 1)

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(opts) do
    table_id = create_table()
    Process.flag(:trap_exit, true)
    attach(table_id)

    state = %{
      table_id: table_id,
      publish_interval: Keyword.get(opts, :publish_interval, @default_publish_interval),
      posthog_opts: Keyword.get(opts, :posthog_opts, [])
    }

    schedule_flush(state.publish_interval)
    Logger.info("Posthog Telemetry Reporter Started")
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    now = DateTime.to_unix(DateTime.utc_now(), :millisecond)

    events =
      :ets.select(state.table_id, [
        {{:"$1", :"$2", :"$3"}, [{:<, :"$1", now}], [{{:"$2", :"$3", :"$1"}}]}
      ])

    if !Enum.empty?(events) do
      merged_events = merge_events(events)

      Sequin.Posthog.batch(merged_events, state.posthog_opts)

      :ets.select_delete(state.table_id, [
        {{:"$1", :"$2", :"$3"}, [{:<, :"$1", now}], [true]}
      ])
    end

    schedule_flush(state.publish_interval)
    {:noreply, state}
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush, interval)
  end

  @impl GenServer
  def terminate(_reason, state) do
    detach()

    events =
      :ets.select(state.table_id, [
        {{:"$1", :"$2", :"$3"}, [], [{{:"$2", :"$3", :"$1"}}]}
      ])

    if !Enum.empty?(events) do
      Sequin.Posthog.batch(events, state.posthog_opts)
    end

    :ets.delete(state.table_id)
  end

  def attach(table_id) do
    :telemetry.attach(
      "sequin-posthog-events",
      [:sequin, :posthog, :event],
      &__MODULE__.handle_event/4,
      table_id
    )
  end

  def detach do
    :telemetry.detach("sequin-posthog-events")
  end

  def handle_event([:sequin, :posthog, :event], %{event: event}, metadata, table_id) do
    timestamp = DateTime.to_unix(DateTime.utc_now(), :millisecond)
    :ets.insert(table_id, {timestamp, event, metadata})
  end

  defp create_table do
    :ets.new(:sequin_posthog_events, [:duplicate_bag, :public, {:write_concurrency, true}])
  end

  # Merges consumer_receive and consumer_ack events by consumer_id
  defp merge_events(events) do
    {mergeable, unmergeable} = split_mergeable_events(events)

    merged =
      mergeable
      |> Enum.group_by(fn {event, metadata, _ts} ->
        {event, metadata.properties.consumer_id}
      end)
      |> Enum.map(fn {_group_by, events} ->
        merge_consumer_events(events)
      end)

    merged ++ unmergeable
  end

  defp split_mergeable_events(events) do
    Enum.split_with(events, fn {event, metadata, _ts} ->
      event in ["consumer_receive", "consumer_ack"] and
        not is_nil(metadata.properties[:consumer_id])
    end)
  end

  defp merge_consumer_events(events) do
    {event, base_metadata, timestamp} = List.first(events)

    total_count = Enum.sum(for {_, metadata, _} <- events, do: metadata.properties.message_count)

    total_bytes =
      Enum.sum(for {_, metadata, _} <- events, do: metadata.properties.bytes_processed)

    {
      event,
      %{
        distinct_id: base_metadata.distinct_id,
        properties: %{
          consumer_id: base_metadata.properties.consumer_id,
          consumer_name: base_metadata.properties.consumer_name,
          event_count: length(events),
          message_count: total_count,
          bytes_processed: total_bytes,
          message_kind: base_metadata.properties.message_kind,
          "$groups": base_metadata.properties[:"$groups"],
          "$process_person_profile": base_metadata.properties[:"$process_person_profile"]
        }
      },
      timestamp
    }
  end
end
