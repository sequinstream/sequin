defmodule Sequin.Extensions.Replication do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.

  Borrowed heavily from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres_cdc_stream/replication.ex
  """
  use Postgrex.ReplicationConnection

  import Bitwise

  alias __MODULE__
  alias Ecto.Adapters.SQL.Sandbox
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
  # alias Sequin.JSON

  require Logger

  def ets_table, do: __MODULE__

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :current_commit_ts, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_xid, nil | integer()
      field :message_handler_ctx, any()
      field :message_handler_module, atom()
      field :id, String.t()
      field :last_committed_lsn, tuple(), default: 0
      field :publication, String.t()
      field :slot_name, String.t()
      field :step, :disconnected | :streaming
      field :test_pid, pid()
    end
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    connection = Keyword.fetch!(opts, :connection)
    publication = Keyword.fetch!(opts, :publication)
    slot_name = Keyword.fetch!(opts, :slot_name)
    test_pid = Keyword.get(opts, :test_pid)
    message_handler_ctx = Keyword.get(opts, :message_handler_ctx)
    message_handler_module = Keyword.fetch!(opts, :message_handler_module)

    rep_conn_opts =
      [auto_reconnect: true, name: via_tuple(id)]
      |> Keyword.merge(connection)
      # Very important. If we don't add this, ReplicationConnection will block start_link (and the
      # calling process!) while it connects.
      |> Keyword.put(:sync_connect, false)

    init = %State{
      id: id,
      publication: publication,
      slot_name: slot_name,
      test_pid: test_pid,
      message_handler_ctx: message_handler_ctx,
      message_handler_module: message_handler_module
    }

    Postgrex.ReplicationConnection.start_link(Replication, init, rep_conn_opts)
  end

  def child_spec(opts) do
    # Not used by DynamicSupervisor, but used by Supervisor in test
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
    Logger.metadata(replication_id: state.id)
    Logger.info("[Replication] Initialized")

    if state.test_pid do
      Mox.allow(Sequin.Mocks.Extensions.ReplicationMessageHandlerMock, state.test_pid, self())
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    {:ok, %{state | step: :disconnected}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_connect(state) do
    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(result, state) do
    Logger.debug("Unknown result: #{inspect(result)}")
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
  # With our current LSN increment strategy, we'll always replay the last record on boot. It seems
  # safe to increment the last_committed_lsn by 1 (Commit also contains the next LSN)
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    messages =
      case reply do
        1 ->
          last_lsn =
            if state.last_committed_lsn do
              lsn_to_int(state.last_committed_lsn)
            else
              # Use the current WAL end if last_committed_lsn is nil
              wal_end
            end

          [<<?r, last_lsn::64, last_lsn::64, last_lsn::64, current_time()::64, 0>>]

        0 ->
          []
      end

    {:noreply, messages, state}
  end

  def handle_data(data, state) do
    Logger.error("Unknown data: #{inspect(data)}")
    {:noreply, state}
  end

  # In Postgres, an LSN is typically represented as a 64-bit integer, but it's sometimes split
  # into two 32-bit parts for easier reading or processing. We'll receive tuples like `{401, 1032909664}`
  # and we'll need to combine them to get the 64-bit LSN.
  defp lsn_to_int(0), do: 0

  defp lsn_to_int({high, low}) do
    high <<< 32 ||| low
  end

  # Used in debugging, worth keeping around.
  # defp lsn_to_tuple(lsn) when is_integer(lsn) do
  #   {lsn >>> 32, lsn &&& 0xFFFFFFFF}
  # end

  defp process_message(%Relation{id: id, columns: columns, namespace: schema, name: table}, %State{} = state) do
    columns =
      Enum.map(columns, fn %{name: name, type: type, flags: flags} ->
        %{name: name, type: type, pk?: Enum.member?(flags, :key)}
      end)

    :ets.insert(ets_table(), {{state.id, id}, columns, schema, table})
    state
  end

  defp process_message(%Begin{commit_timestamp: ts, final_lsn: lsn, xid: xid}, %State{} = state) do
    %{state | current_commit_ts: ts, current_xaction_lsn: lsn, current_xid: xid}
  end

  # Ensure we do not have an out-of-order bug by asserting equality
  defp process_message(
         %Commit{lsn: lsn, commit_timestamp: ts},
         %State{current_xaction_lsn: current_lsn, current_commit_ts: ts, id: id} = state
       )
       when current_lsn == lsn do
    :ets.insert(ets_table(), {{id, :last_committed_at}, ts})
    %{state | last_committed_lsn: lsn, current_xaction_lsn: nil, current_xid: nil, current_commit_ts: nil}
  end

  defp process_message(%Insert{} = msg, state) do
    [{_, columns, schema, table}] = :ets.lookup(ets_table(), {state.id, msg.relation_id})

    record = %NewRecord{
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: data_tuple_to_ids(columns, msg.tuple_data),
      schema: schema,
      table: table,
      record: data_tuple_to_map(columns, msg.tuple_data),
      type: "insert"
    }

    handle_message(state, record)

    state
  end

  # If replication mode is default (not full), we will not get old_tuple_data:
  # msg: %Sequin.Extensions.PostgresAdapter.Decoder.Messages.Update{
  #   relation_id: 15465878,
  #   changed_key_tuple_data: nil,
  #   old_tuple_data: nil,
  #   tuple_data: {"1", "Chani", "Atreides", "Arrakis"}
  # },
  defp process_message(%Update{} = msg, %State{} = state) do
    [{_, columns, schema, table}] = :ets.lookup(ets_table(), {state.id, msg.relation_id})

    old_record =
      if msg.old_tuple_data do
        data_tuple_to_map(columns, msg.old_tuple_data)
      end

    record = %UpdatedRecord{
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: data_tuple_to_ids(columns, msg.tuple_data),
      schema: schema,
      table: table,
      old_record: old_record,
      record: data_tuple_to_map(columns, msg.tuple_data),
      type: "update"
    }

    handle_message(state, record)

    state
  end

  # When the publication mode is default and we don't get old_tpule_data, we'll get this (first
  # element is PK):
  # msg: %Sequin.Extensions.PostgresAdapter.Decoder.Messages.Delete{
  #   relation_id: 15465677,
  #   changed_key_tuple_data: {"1", nil, nil, nil},
  #   old_tuple_data: nil
  # },
  # Otherwise, in full mode, we'll get old_tuple_data.

  defp process_message(%Delete{} = msg, %State{} = state) do
    [{_, columns, schema, table}] = :ets.lookup(ets_table(), {state.id, msg.relation_id})

    prev_tuple_data =
      if msg.old_tuple_data do
        msg.old_tuple_data
      else
        msg.changed_key_tuple_data
      end

    record = %DeletedRecord{
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: data_tuple_to_ids(columns, prev_tuple_data),
      schema: schema,
      table: table,
      old_record: data_tuple_to_map(columns, prev_tuple_data),
      type: "delete"
    }

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
    state.message_handler_module.handle_message(state.message_handler_ctx, change)

    if state.test_pid do
      send(state.test_pid, {Replication, :message_handled})
    end
  end

  def data_tuple_to_ids(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.filter(fn {col, _} -> col.pk? end)
    |> Enum.map(fn {_, value} -> value end)
  end

  def data_tuple_to_map(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Map.new(fn {%{name: name, type: type}, value} ->
      {name, cast_value(type, value)}
    end)
  end

  defp cast_value(type, value) do
    case Ecto.Type.cast(string_to_ecto_type(type), value) do
      {:ok, casted_value} -> casted_value
      # Fallback to original value if casting fails
      :error -> value
    end
  end

  @postgres_to_ecto_type_mapping %{
    # Numeric Types
    "int2" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "float4" => :float,
    "float8" => :float,
    "numeric" => :decimal,
    "money" => :decimal,
    # Character Types
    "char" => :string,
    "varchar" => :string,
    "text" => :string,
    # Binary Data Types
    "bytea" => :binary,
    # Date/Time Types
    "timestamp" => :naive_datetime,
    "timestamptz" => :utc_datetime,
    "date" => :date,
    "time" => :time,
    "timetz" => :time,
    # Ecto doesn't have a direct interval type
    "interval" => :map,
    # Boolean Type
    "bool" => :boolean,
    # Geometric Types
    "point" => {:array, :float},
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,
    # Network Address Types
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,
    # Bit String Types
    "bit" => :string,
    "bit_varying" => :string,
    # Text Search Types
    "tsvector" => :string,
    "tsquery" => :string,
    # UUID Type
    "uuid" => Ecto.UUID,
    # XML Type
    "xml" => :string,
    # JSON Types
    "json" => :map,
    "jsonb" => :map,
    # Arrays
    "array" => {:array, :any},
    # Composite Types
    "composite" => :map,
    # Range Types
    "range" => {:array, :any},
    # Domain Types
    "domain" => :any,
    # Object Identifier Types
    "oid" => :integer,
    # pg_lsn Type
    "pg_lsn" => :string,
    # Pseudotypes
    "any" => :any
  }

  defp string_to_ecto_type(type) do
    Map.get(@postgres_to_ecto_type_mapping, type, :string)
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch

  # Add this function to get the last committed timestamp
  def get_last_committed_at(id) do
    case :ets.lookup(ets_table(), {id, :last_committed_at}) do
      [{{^id, :last_committed_at}, ts}] -> ts
      [] -> nil
    end
  end
end
