defmodule Sequin.Tracer.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Replication.Message
  alias Sequin.Tracer.State
  alias Sequin.Tracer.State.ConsumerTrace
  alias Sequin.Tracer.State.TraceMessage

  require Logger

  typedstruct do
    field :account_id, String.t()
    field :trace_messages, list(TraceMessage.t()), default: []
  end

  defmodule TraceMessage do
    @moduledoc false
    use TypedStruct

    alias Sequin.Tracer.State.ConsumerTrace

    typedstruct do
      field :message, Message.t()
      field :consumer_traces, list(ConsumerTrace.t()), default: []
    end
  end

  defmodule ConsumerTrace do
    @moduledoc false
    use TypedStruct

    alias Sequin.Tracer.State.TraceEvent

    typedstruct do
      field :consumer_id, String.t()
      field :events, list(TraceEvent.t()), default: []
    end
  end

  defmodule TraceEvent do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :type, atom()
      field :timestamp, DateTime.t()
      field :metadata, map(), default: %{}
    end
  end

  def new(account_id) do
    %__MODULE__{account_id: account_id, trace_messages: []}
  end

  def message_replicated(%State{} = state, %Message{} = message) do
    new_trace_message = %TraceMessage{
      message: message,
      consumer_traces: []
    }

    %{state | trace_messages: [new_trace_message | state.trace_messages]}
  end

  def message_filtered(%State{} = state, consumer_id, %Message{} = message) do
    update_trace_message(state, message, fn tm ->
      add_consumer_event(tm, consumer_id, create_trace_event(:filtered))
    end)
  end

  def messages_ingested(%State{} = state, consumer_id, events_or_records) do
    Enum.reduce(
      events_or_records,
      state,
      &update_trace_message(&2, &1, fn tm ->
        add_consumer_event(tm, consumer_id, create_trace_event(:ingested, %{id: &1.id}))
      end)
    )
  end

  def messages_received(%State{} = state, consumer_id, events_or_records) do
    Enum.reduce(
      events_or_records,
      state,
      &update_trace_message(&2, &1, fn tm ->
        add_consumer_event(tm, consumer_id, create_trace_event(:received, %{ack_id: &1.ack_id}))
      end)
    )
  end

  def messages_acked(%State{} = state, consumer_id, ack_ids) do
    Enum.reduce(
      ack_ids,
      state,
      &update_trace_message(&2, [ack_id: &1], fn tm ->
        add_consumer_event(tm, consumer_id, create_trace_event(:acked))
      end)
    )
  end

  def reset(%State{} = state) do
    %{state | trace_messages: []}
  end

  defp update_trace_message(%State{} = state, identifier, update_fn) do
    case find_trace_message_index(state, identifier) do
      nil ->
        Logger.warning("Trace message not found for #{inspect(identifier)}")
        state

      index ->
        trace_messages = List.update_at(state.trace_messages, index, update_fn)
        %{state | trace_messages: trace_messages}
    end
  end

  defp find_trace_message_index(%State{} = state, %Message{} = message) do
    Enum.find_index(state.trace_messages, fn tm -> tm.message.trace_id == message.trace_id end)
  end

  defp find_trace_message_index(%State{} = state, ack_id: ack_id) do
    Enum.find_index(state.trace_messages, fn tm -> matches_by_ack_id?(tm, ack_id) end)
  end

  defp find_trace_message_index(%State{} = state, event_or_record) do
    Enum.find_index(state.trace_messages, fn tm ->
      tm.message.trace_id == event_or_record.replication_message_trace_id
    end)
  end

  defp matches_by_ack_id?(%TraceMessage{consumer_traces: consumer_traces}, ack_id) do
    Enum.any?(consumer_traces, fn consumer_trace ->
      Enum.any?(consumer_trace.events, fn event ->
        event.metadata[:ack_id] == ack_id
      end)
    end)
  end

  defp create_trace_event(type, metadata \\ %{}) do
    %TraceEvent{
      type: type,
      timestamp: DateTime.utc_now(),
      metadata: metadata
    }
  end

  defp add_consumer_event(%TraceMessage{} = tm, consumer_id, event) do
    consumer_trace =
      Enum.find(tm.consumer_traces, &(&1.consumer_id == consumer_id)) ||
        %ConsumerTrace{consumer_id: consumer_id, events: []}

    updated_consumer_trace = %{consumer_trace | events: [event | consumer_trace.events]}

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
end
