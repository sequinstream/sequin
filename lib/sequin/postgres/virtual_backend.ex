defmodule Sequin.Postgres.VirtualBackend do
  @moduledoc """
  Virtual Postgres backend that delegates to a configurable source.

  This backend adapts any `Sequin.Postgres.Source` implementation to work
  with SlotProducer, enabling benchmarking and testing without a real
  Postgres connection.

  ## Usage

      # Start the source server first
      {:ok, _pid} = BenchmarkSource.start_link(id: some_id, ...)

      # Pass to SlotProducer with source_mod in connect_opts
      SlotProducer.start_link(
        backend_mod: Sequin.Postgres.VirtualBackend,
        connect_opts: [id: some_id, source_mod: BenchmarkSource],
        ...
      )
  """

  @behaviour Sequin.Postgres.Backend

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :id, term(), enforce: true
      field :source_mod, module(), enforce: true
    end
  end

  @impl true
  def connect(opts) do
    id = Keyword.fetch!(opts, :id)
    source_mod = Keyword.fetch!(opts, :source_mod)
    # Register the producer with the source - this will trigger the first :tcp message
    # We pass self() since we're running in the SlotProducer process context
    source_mod.set_producer(id, self())
    {:ok, %State{id: id, source_mod: source_mod}}
  end

  @impl true
  def handle_streaming(_query, state) do
    # No-op for virtual backend - we're always "streaming"
    {:ok, state}
  end

  @impl true
  def checkin(state) do
    # No-op - the source handles sending :tcp messages
    {:ok, state}
  end

  @impl true
  def handle_copy_recv(_socket_msg, max, %State{} = state) do
    copies = state.source_mod.recv_copies(state.id, max)
    {:ok, copies, state}
  end

  @impl true
  def handle_copy_send(msg, %State{} = state) do
    state.source_mod.handle_ack(state.id, msg)
    :ok
  end

  @impl true
  def handle_simple(_query, _params, state) do
    # Return a fake restart_lsn for init_restart_wal_cursor
    fake_result = %Postgrex.Result{
      rows: [["0/1"]],
      columns: ["restart_lsn"],
      num_rows: 1
    }

    {:ok, [fake_result], state}
  end

  @impl true
  def disconnect(_reason, _state) do
    :ok
  end
end
