defmodule Sequin.Extensions.Replication do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.

  Borrowed heavily from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres_cdc_stream/replication.ex
  """

  use Postgrex.ReplicationConnection

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
      field :slot_name, String.t()
      field :publication, String.t()
      field :tid, :ets.tid()
      field :step, :disconnected | :streaming
      field :ts, nil | integer()
    end
  end

  def start_link(args) do
    connection = Keyword.fetch!(args, :connection)
    publication = Keyword.fetch!(args, :publication)
    slot_name = Keyword.fetch!(args, :slot_name)

    opts = Keyword.merge([auto_reconnect: true], connection)

    init = %State{
      publication: publication,
      slot_name: slot_name
    }

    Postgrex.ReplicationConnection.start_link(__MODULE__, init, opts)
  end

  @impl Postgrex.ReplicationConnection
  def handle_connect(%State{} = state) do
    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(result, state) do
    Logger.error("Unknown result: #{inspect(result)}")
    {:noreply, state}
  end

  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @impl Postgrex.ReplicationConnection
  def init(%State{} = state) do
    tid = :ets.new(__MODULE__, [:public, :set])
    {:ok, %{state | tid: tid, step: :disconnected}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_data(<<?w, _header::192, msg::binary>>, state) do
    new_state =
      msg
      |> Decoder.decode_message()
      |> process_message(state)

    {:noreply, new_state}
  end

  # keepalive
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
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

  defp process_message(%Relation{id: id, columns: columns, namespace: schema, name: table}, state) do
    columns =
      Enum.map(columns, fn %{name: name, type: type} ->
        %{name: name, type: type}
      end)

    :ets.insert(state.tid, {id, columns, schema, table})
    state
  end

  defp process_message(%Begin{commit_timestamp: ts}, state) do
    %{state | ts: ts}
  end

  defp process_message(%Commit{}, state) do
    %{state | ts: nil}
  end

  defp process_message(%Insert{} = msg, state) do
    Logger.debug("Got message: #{inspect(msg)}")
    [{_, columns, schema, table}] = :ets.lookup(state.tid, msg.relation_id)

    record = %NewRecord{
      commit_timestamp: state.ts,
      errors: nil,
      schema: schema,
      table: table,
      record: data_tuple_to_map(columns, msg.tuple_data),
      type: "insert"
    }

    publish(record)

    state
  end

  defp process_message(%Update{} = msg, state) do
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
      commit_timestamp: state.ts,
      errors: nil,
      schema: schema,
      table: table,
      old_record: data_tuple_to_map(columns, msg.old_tuple_data),
      record: data_tuple_to_map(columns, msg.tuple_data),
      type: "update"
    }

    # -    broadcast(record, state.tenant)

    publish(record)

    state
  end

  defp process_message(%Delete{} = msg, state) do
    Logger.debug("Got message: #{inspect(msg)}")
    [{_, columns, schema, table}] = :ets.lookup(state.tid, msg.relation_id)

    record = %DeletedRecord{
      commit_timestamp: state.ts,
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

    publish(record)

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
  def publish(change) do
    # # -    |> Enum.each(fn params ->
    # # -      inspect({params, change})
    # # -      # Phoenix.PubSub.broadcast_from(
    # # -      #   Realtime.PubSub,
    # # -      #   self(),
    # # -      #   PostgresCdcStream.topic(tenant, params),
    # # -      #   change,
    # # -      #   PostgresCdcStream.MessageDispatcher
    # # -      # )
    # # -    end)
    # kind =
    #   case change do
    #     %NewRecord{} -> "insert"
    #     %UpdatedRecord{} -> "update"
    #     %DeletedRecord{} -> "delete"
    #   end

    # json =
    change
    |> JSON.encode_struct_with_type()
    |> Jason.encode!()

    # TODO
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
