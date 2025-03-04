defmodule Sequin.Tracer.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Tracer.State
  alias Sequin.Tracer.State.ConsumerTrace
  alias Sequin.Tracer.State.MessageTrace

  require Logger

  typedstruct do
    field :account_id, String.t()
    field :message_traces, list(MessageTrace.t()), default: []
    field :started_at, DateTime.t()
  end

  defmodule MessageTrace do
    @moduledoc false
    use TypedStruct

    alias Sequin.Tracer.State.ConsumerTrace

    typedstruct do
      field :message, Message.t()
      field :replicated_at, DateTime.t()
      field :database_id, String.t()
      field :consumer_traces, list(ConsumerTrace.t()), default: []
    end
  end

  defmodule ConsumerTrace do
    @moduledoc false
    use TypedStruct

    alias Sequin.Tracer.State.TraceSpan

    typedstruct do
      field :consumer_id, String.t()
      field :spans, list(TraceSpan.t()), default: []
    end
  end

  defmodule TraceSpan do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :type, atom()
      field :timestamp, DateTime.t()
      field :duration, integer()
      field :metadata, map(), default: %{}
    end
  end

  @max_message_trace_limit 1000

  def new(account_id) do
    %__MODULE__{
      account_id: account_id,
      message_traces: [],
      started_at: DateTime.utc_now()
    }
  end

  def message_replicated(%State{} = state, %PostgresDatabase{} = db, %Message{} = message) do
    new_message_trace = %MessageTrace{
      message: message,
      database_id: db.id,
      consumer_traces: [],
      replicated_at: DateTime.utc_now()
    }

    updated_message_traces = Enum.take([new_message_trace | state.message_traces], @max_message_trace_limit)

    %{state | message_traces: updated_message_traces}
  end

  def message_filtered(%State{} = state, consumer_id, %Message{} = message) do
    update_message_trace(state, message, fn tm ->
      add_consumer_event(tm, consumer_id, create_trace_event(:filtered))
    end)
  end

  def messages_ingested(%State{} = state, consumer_id, events_or_records) do
    Enum.reduce(
      events_or_records,
      state,
      &update_message_trace(&2, &1, fn tm ->
        add_consumer_event(tm, consumer_id, create_trace_event(:ingested, %{id: &1.id}))
      end)
    )
  end

  def messages_received(%State{} = state, consumer_id, events_or_records) do
    Enum.reduce(
      events_or_records,
      state,
      &update_message_trace(&2, &1, fn tm ->
        add_consumer_event(tm, consumer_id, create_trace_event(:received, %{ack_id: &1.ack_id}))
      end)
    )
  end

  def messages_acked(%State{} = state, consumer_id, ack_ids) do
    Enum.reduce(
      ack_ids,
      state,
      &update_message_trace(&2, [ack_id: &1], fn tm ->
        add_consumer_event(tm, consumer_id, create_trace_event(:acked))
      end)
    )
  end

  def reset(%State{} = state) do
    %{state | message_traces: []}
  end

  defp update_message_trace(%State{} = state, identifier, update_fn) do
    case find_message_trace_index(state, identifier) do
      nil ->
        state

      index ->
        message_traces = List.update_at(state.message_traces, index, update_fn)
        %{state | message_traces: message_traces}
    end
  end

  defp find_message_trace_index(%State{} = state, %Message{} = message) do
    Enum.find_index(state.message_traces, fn tm -> tm.message.trace_id == message.trace_id end)
  end

  defp find_message_trace_index(%State{} = state, ack_id: ack_id) do
    Enum.find_index(state.message_traces, fn tm -> matches_by_ack_id?(tm, ack_id) end)
  end

  defp find_message_trace_index(%State{} = state, %ConsumerEvent{} = event) do
    Enum.find_index(state.message_traces, fn tm ->
      tm.message.trace_id == event.replication_message_trace_id
    end)
  end

  defp find_message_trace_index(%State{} = state, %ConsumerRecord{} = record) do
    Enum.find_index(state.message_traces, fn tm ->
      tm.message.trace_id == record.replication_message_trace_id
    end)
  end

  defp matches_by_ack_id?(%MessageTrace{consumer_traces: consumer_traces}, ack_id) do
    Enum.any?(consumer_traces, fn consumer_trace ->
      Enum.any?(consumer_trace.spans, fn event ->
        event.metadata[:ack_id] == ack_id
      end)
    end)
  end

  defp create_trace_event(type, metadata \\ %{}) do
    %TraceSpan{
      type: type,
      timestamp: DateTime.utc_now(),
      metadata: metadata
    }
  end

  defp add_consumer_event(%MessageTrace{} = tm, consumer_id, event) do
    consumer_trace =
      Enum.find(tm.consumer_traces, &(&1.consumer_id == consumer_id)) ||
        %ConsumerTrace{consumer_id: consumer_id, spans: []}

    last_span = List.first(consumer_trace.spans)
    last_timestamp = if last_span, do: last_span.timestamp, else: tm.replicated_at
    duration = DateTime.diff(event.timestamp, last_timestamp, :millisecond)
    event = %{event | duration: duration}

    updated_consumer_trace = %{consumer_trace | spans: [event | consumer_trace.spans]}

    updated_consumer_traces =
      if Enum.any?(tm.consumer_traces, &(&1.consumer_id == consumer_id)) do
        Enum.map(tm.consumer_traces, fn ct ->
          if ct.consumer_id == consumer_id, do: updated_consumer_trace, else: ct
        end)
      else
        [updated_consumer_trace | tm.consumer_traces]
      end

    %{tm | consumer_traces: updated_consumer_traces}
  end

  def erase_fields(%Message{} = message) do
    %Message{message | fields: nil, old_fields: nil}
  end
end
