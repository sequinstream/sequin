defmodule Sequin.Extensions.Replication do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.

  Borrowed heavily from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres_cdc_stream/replication.ex
  """

  use Postgrex.ReplicationConnection

  alias __MODULE__
  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Extensions.PostgresAdapter.Decoder
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Begin
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Commit
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Delete
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Insert
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Update
  alias Sequin.JSON

  require Logger

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :current_xid, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_commit_ts, nil | integer()
      field :last_committed_lsn, integer(), default: 0
      field :publication, String.t()
      field :slot_name, String.t()
      field :step, :disconnected | :streaming
      field :test_pid, pid()
      field :tid, :ets.tid()
      field :handle_message_fun, (map() -> any())
    end
  end

  def start_link(args) do
    id = Keyword.fetch!(args, :id)
    connection = Keyword.fetch!(args, :connection)
    publication = Keyword.fetch!(args, :publication)
    slot_name = Keyword.fetch!(args, :slot_name)
    test_pid = Keyword.get(args, :test_pid)
    handle_message_fun = Keyword.get(args, :handle_message_fun)

    opts = Keyword.merge([auto_reconnect: true, name: via_tuple(id)], connection)

    init = %State{
      publication: publication,
      slot_name: slot_name,
      test_pid: test_pid,
      handle_message_fun: handle_message_fun
    }

    Postgrex.ReplicationConnection.start_link(Replication, init, opts)
  end

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)

    %{
      id: via_tuple(id),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def via_tuple(id) do
    {:via, Registry, {Sequin.Registry, {Replication, id}}}
  end

  @impl Postgrex.ReplicationConnection
  def init(%State{} = state) do
    tid = :ets.new(Replication, [:public, :set])
    {:ok, %{state | tid: tid, step: :disconnected}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_connect(state) do
    query = "CREATE_REPLICATION_SLOT #{state.slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :create_slot}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result([%Postgrex.Result{command: :create}], %{step: :create_slot} = state) do
    if state.test_pid do
      send(state.test_pid, {Replication, :slot_created})
    end

    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(%Postgrex.Error{postgres: %{code: :duplicate_object}}, %{step: :create_slot} = state) do
    if state.test_pid do
      send(state.test_pid, {Replication, :slot_created})
    end

    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  def handle_result(result, state) do
    Logger.error("Unknown result: #{inspect(result)}")
    {:noreply, state}
  end

  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @impl Postgrex.ReplicationConnection
  def handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    next_state =
      msg
      |> Decoder.decode_message()
      |> process_message(state)

    {:noreply, next_state}
  end

  # keepalive
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, %State{} = state) do
    messages =
      case reply do
        1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
        0 -> []
      end

    {:noreply, messages, state}
  end

  def handle_data(data, state) do
    Logger.error("Unknown data: #{inspect(data)}")
    {:noreply, state}
  end

  defp process_message(%Relation{id: id, columns: columns, namespace: schema, name: table}, %State{} = state) do
    columns =
      Enum.map(columns, fn %{name: name, type: type} ->
        %{name: name, type: type}
      end)

    :ets.insert(state.tid, {id, columns, schema, table})
    state
  end

  defp process_message(%Begin{commit_timestamp: ts, final_lsn: lsn, xid: xid}, %State{} = state) do
    %{state | current_commit_ts: ts, current_xaction_lsn: lsn, current_xid: xid}
  end

  # Ensure we do not have an out-of-order bug by asserting equality
  defp process_message(
         %Commit{lsn: lsn, commit_timestamp: ts},
         %State{current_xaction_lsn: lsn, current_commit_ts: ts} = state
       ) do
    %{state | last_committed_lsn: lsn, current_xaction_lsn: nil, current_xid: nil, current_commit_ts: nil}
  end

  defp process_message(%Insert{} = msg, state) do
    Logger.debug("Got message: #{inspect(msg)}")
    [{_, columns, schema, table}] = :ets.lookup(state.tid, msg.relation_id)

    record = %NewRecord{
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      schema: schema,
      table: table,
      record: data_tuple_to_map(columns, msg.tuple_data),
      type: "insert"
    }

    handle_message(state, record)

    state
  end

  defp process_message(%Update{} = msg, %State{} = state) do
    Logger.debug("Got message: #{inspect(msg)}")
    [{_, columns, schema, table}] = :ets.lookup(state.tid, msg.relation_id)

    # record = %UpdatedRecord{
    #   -      columns: columns,
    #          commit_timestamp: state.ts,
    #          errors: nil,
    #          schema: schema,
    #          table: table,
    #          old_record: data_tuple_to_map(columns, msg.old_tuple_data),
    #          record: data_tuple_to_map(columns, msg.tuple_data),
    #   -      type: "UPDATE"
    #   +      type: "update"
    #        }

    record = %UpdatedRecord{
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      schema: schema,
      table: table,
      old_record: data_tuple_to_map(columns, msg.old_tuple_data),
      record: data_tuple_to_map(columns, msg.tuple_data),
      type: "update"
    }

    handle_message(state, record)

    state
  end

  defp process_message(%Delete{} = msg, %State{} = state) do
    Logger.debug("Got message: #{inspect(msg)}")
    [{_, columns, schema, table}] = :ets.lookup(state.tid, msg.relation_id)

    record = %DeletedRecord{
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      schema: schema,
      table: table,
      old_record: data_tuple_to_map(columns, msg.old_tuple_data),
      type: "delete"
    }

    # record = %DeletedRecord{
    #   -      columns: columns,
    #          commit_timestamp: state.ts,
    #          errors: nil,
    #          schema: schema,
    #          table: table,
    #          old_record: data_tuple_to_map(columns, msg.old_tuple_data),
    #   -      type: "UPDATE"
    #   +      type: "delete"
    #        }

    #   -    broadcast(record, state.tenant)
    #   +    publish(record)

    handle_message(state, record)

    state
  end

  defp process_message(msg, state) do
    Logger.error("Unknown message: #{inspect(msg)}")
    state
  end

  # Example change event
  # %Extensions.PostgresAdapter.Changes.NewRecord{
  #   columns: [
  #     %{name: "id", type: "int4"},
  #     %{name: "first_name", type: "text"},
  #     %{name: "last_name", type: "text"}
  #   ],
  #   commit_timestamp: ~U[2024-03-01 16:11:32.272722Z],
  #   errors: nil,
  #   schema: "__test_cdc__",
  #   table: "test_table",
  #   record: %{"first_name" => "Paul", "id" => "1", "last_name" => "Atreides"},
  #   subscription_ids: nil,
  #   type: "UPDATE"
  # }
  defp handle_message(%State{} = state, change) do
    if state.handle_message_fun do
      # For DI in test
      state.handle_message_fun.(change)
    else
      change
      |> JSON.encode_struct_with_type()
      |> Jason.encode!()

      :ok
    end
  end

  def data_tuple_to_map(column, tuple_data) do
    column
    |> Enum.with_index()
    |> Enum.reduce_while(%{}, fn {column_map, index}, acc ->
      case column_map do
        %{name: column_name, type: column_type}
        when is_binary(column_name) and is_binary(column_type) ->
          res =
            try do
              {:ok, elem(tuple_data, index)}
            rescue
              ArgumentError -> :error
            end

          case res do
            {:ok, record} ->
              {:cont, Map.put(acc, column_name, convert_column_record(record, column_type))}

            :error ->
              {:halt, acc}
          end

        _ ->
          {:cont, acc}
      end
    end)
  end

  defp convert_column_record(record, "timestamp") when is_binary(record) do
    case Sequin.Time.parse_timestamp(record) do
      {:ok, datetime} -> DateTime.to_iso8601(datetime)
      _ -> record
    end
  end

  defp convert_column_record(record, "timestamptz") when is_binary(record) do
    case Sequin.Time.parse_timestamp(record) do
      {:ok, datetime} -> DateTime.to_iso8601(datetime)
      _ -> record
    end
  end

  defp convert_column_record(record, _column_type) do
    record
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end
