defmodule Sequin.Runtime.SlotProducer.Processor do
  @moduledoc """
  A simple GenStage consumer with manual demand for testing sync_info behavior.
  """

  @behaviour Sequin.Runtime.SlotProducer.ProcessorBehaviour

  use GenStage

  alias Sequin.Error.ServiceError
  alias Sequin.Postgres.ValueCaster
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Delete
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Insert
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Origin
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Truncate
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Type
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Update
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProcessor.Message.Field
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Message
  alias Sequin.Runtime.SlotProducer.ProcessorBehaviour
  alias Sequin.Runtime.SlotProducer.Relation
  alias Sequin.Runtime.SlotProducer.ReorderBuffer

  require Logger

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def config(key) do
    Keyword.fetch!(config(), key)
  end

  def config do
    Application.fetch_env!(:sequin, __MODULE__)
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Runtime.SlotProducer.Relation

    typedstruct do
      field :relations, %{required(table_oid :: String.t()) => Relation.t()}, default: %{}
      field :id, String.t()
      field :partition_idx, non_neg_integer()
      field :reorder_buffer, pid() | nil
    end
  end

  def partition_count do
    System.schedulers_online()
  end

  def partitions do
    0..(partition_count() - 1)
  end

  def start_link(opts \\ []) do
    id = Keyword.fetch!(opts, :id)
    partition_idx = Keyword.fetch!(opts, :partition_idx)
    GenStage.start_link(__MODULE__, opts, name: via_tuple(id, partition_idx))
  end

  def via_tuple(id, partition_idx) do
    {:via, :syn, {:replication, {__MODULE__, id, partition_idx}}}
  end

  @impl ProcessorBehaviour
  def handle_relation(pid, relation) when is_pid(pid) do
    GenStage.cast(pid, {:relation, relation})
  end

  def handle_relation(id, relation) do
    Enum.each(partitions(), fn partition_idx ->
      GenStage.cast(via_tuple(id, partition_idx), {:relation, relation})
    end)
  end

  @impl ProcessorBehaviour
  def handle_batch_marker(pid, batch) when is_pid(pid) do
    GenStage.cast(pid, {:batch_marker, batch})
  end

  def handle_batch_marker(id, batch) do
    Enum.each(partitions(), fn partition_idx ->
      GenStage.cast(via_tuple(id, partition_idx), {:batch_marker, batch})
    end)
  end

  @impl GenStage
  def init(opts) do
    id = Keyword.fetch!(opts, :id)

    state = %State{
      id: id,
      partition_idx: Keyword.fetch!(opts, :partition_idx)
    }

    # Get subscription configuration - default to empty for manual subscriptions
    {:producer_consumer, state, subscribe_to: Keyword.get(opts, :subscribe_to, [])}
  end

  @impl GenStage
  def handle_subscribe(:consumer, _opts, {pid, _ref}, state) do
    # ReorderBuffer subscribing to us
    {:automatic, %{state | reorder_buffer: pid}}
  end

  def handle_subscribe(:producer, _opts, _producer, state) do
    # We're subscribing to SlotProducer - automatic subscription
    {:automatic, state}
  end

  @impl GenStage
  def handle_events(events, _from, state) do
    messages =
      events
      |> Enum.map(fn %Message{} = msg ->
        case Decoder.decode_message(msg.payload) do
          %type{} = payload when type in [Insert, Update, Delete, LogicalMessage] ->
            %{msg | message: cast_message(payload, msg, state.relations)}

          %type{} = _msg when type in [Origin, Truncate, Type] ->
            nil

          msg ->
            Logger.warning("Unhandled message type: #{inspect(msg)}")
            nil
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.reject(&internal_change?/1)

    {:noreply, messages, state}
  end

  @impl GenStage
  def handle_cast({:relation, %Relation{} = relation}, state) do
    {:noreply, [], %{state | relations: Map.put(state.relations, relation.id, relation)}}
  end

  def handle_cast({:batch_marker, %BatchMarker{} = marker}, state) do
    if is_nil(state.reorder_buffer) do
      raise "Received batch marker without a reorder buffer"
    end

    ReorderBuffer.handle_batch_marker(state.id, {marker, state.partition_idx})

    {:noreply, [], state}
  end

  @spec cast_message(decoded_message :: map(), envelope :: Message.t(), schemas :: map()) :: Message.t() | map()
  defp cast_message(%Insert{} = payload, %Message{} = envelope, schemas) do
    %{columns: columns, schema: schema, table: table, parent_table_id: parent_table_id} =
      Map.get(schemas, payload.relation_id)

    ids = data_tuple_to_ids(columns, payload.tuple_data)

    %SlotProcessor.Message{
      action: :insert,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: parent_table_id,
      fields: data_tuple_to_fields(ids, columns, payload.tuple_data),
      trace_id: UUID.uuid4(),
      commit_lsn: envelope.commit_lsn,
      commit_idx: envelope.commit_idx,
      commit_timestamp: envelope.commit_ts,
      transaction_annotations: envelope.transaction_annotations,
      byte_size: envelope.byte_size,
      batch_epoch: envelope.batch_epoch
    }
  end

  defp cast_message(%Update{} = payload, %Message{} = envelope, schemas) do
    %{columns: columns, schema: schema, table: table, parent_table_id: parent_table_id} =
      Map.get(schemas, payload.relation_id)

    ids = data_tuple_to_ids(columns, payload.tuple_data)

    old_fields =
      if payload.old_tuple_data do
        data_tuple_to_fields(ids, columns, payload.old_tuple_data)
      end

    %SlotProcessor.Message{
      action: :update,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: parent_table_id,
      old_fields: old_fields,
      fields: data_tuple_to_fields(ids, columns, payload.tuple_data),
      trace_id: UUID.uuid4(),
      commit_lsn: envelope.commit_lsn,
      commit_idx: envelope.commit_idx,
      commit_timestamp: envelope.commit_ts,
      transaction_annotations: envelope.transaction_annotations,
      byte_size: envelope.byte_size,
      batch_epoch: envelope.batch_epoch
    }
  end

  defp cast_message(%Delete{} = payload, %Message{} = envelope, schemas) do
    %{columns: columns, schema: schema, table: table, parent_table_id: parent_table_id} =
      Map.get(schemas, payload.relation_id)

    prev_tuple_data =
      if payload.old_tuple_data do
        payload.old_tuple_data
      else
        payload.changed_key_tuple_data
      end

    ids = data_tuple_to_ids(columns, prev_tuple_data)

    %SlotProcessor.Message{
      action: :delete,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: parent_table_id,
      old_fields: data_tuple_to_fields(ids, columns, prev_tuple_data),
      trace_id: UUID.uuid4(),
      commit_lsn: envelope.commit_lsn,
      commit_idx: envelope.commit_idx,
      commit_timestamp: envelope.commit_ts,
      transaction_annotations: envelope.transaction_annotations,
      byte_size: envelope.byte_size,
      batch_epoch: envelope.batch_epoch
    }
  end

  defp cast_message(%LogicalMessage{} = payload, _envelope, _relations) do
    payload
  end

  defp internal_change?(%struct{} = msg) when struct in [Insert, Update, Delete] do
    msg.table_schema in [@config_schema, @stream_schema] and msg.table_schema != "public"
  end

  defp internal_change?(_msg), do: false

  def data_tuple_to_ids(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.filter(fn {col, _} -> col.pk? end)
    # Very important - the system expects the PKs to be sorted by attnum
    |> Enum.sort_by(fn {col, _} -> col.attnum end)
    |> Enum.map(fn {_, value} -> value end)
  end

  @spec data_tuple_to_fields(list(), [map()], tuple()) :: [Field.t()]
  def data_tuple_to_fields(id_list, columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.map(fn {%{name: name, attnum: attnum, type: type}, value} ->
      case ValueCaster.cast(type, value) do
        {:ok, casted_value} ->
          %Field{
            column_name: name,
            column_attnum: attnum,
            value: casted_value
          }

        {:error, %ServiceError{code: :invalid_json} = error} ->
          details = Map.put(error.details, :ids, id_list)

          raise %{
            error
            | details: details,
              message: error.message <> " for column `#{name}` in row with ids: #{inspect(id_list)}"
          }
      end
    end)
  end
end
