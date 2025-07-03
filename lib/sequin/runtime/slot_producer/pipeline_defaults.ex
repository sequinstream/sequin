defmodule Sequin.Runtime.SlotProducer.PipelineDefaults do
  @moduledoc """
  Default callback implementations for the SlotProducer pipeline.
  """

  @behaviour Sequin.Runtime.SlotProducer
  @behaviour Sequin.Runtime.SlotProducer.ReorderBuffer

  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.SlotProducer
  alias Sequin.Runtime.SlotProducer.Batch
  alias Sequin.Runtime.SlotProducer.Processor
  alias Sequin.Runtime.SlotProducer.ReorderBuffer
  alias Sequin.Workers.CreateReplicationSlotWorker

  def processor_mod do
    Processor
  end

  @doc "Default implementation for ReorderBuffer's flush_batch_fn callback"
  @impl ReorderBuffer
  def flush_batch(id, %Batch{} = batch) do
    SlotProcessorServer.handle_batch(id, batch)
  end

  @doc "Default implementation for SlotProducer's restart_wal_cursor_fn callback"
  @impl SlotProducer
  def restart_wal_cursor(id, _current_cursor) do
    SlotProcessorServer.restart_wal_cursor(id)
  end

  @impl SlotProducer
  def on_connect_fail(%SlotProducer.State{} = state, error) do
    conn = state.conn.()

    error_or_error_msg =
      with {:ok, %{"active" => false}} <- Postgres.get_replication_slot(conn, state.slot_name),
           {:ok, _pub} <- Postgres.get_publication(conn, state.publication_name) do
        cond do
          match?(%Postgrex.Error{postgres: %{code: :undefined_object, routine: "get_publication_oid"}}, error) ->
            # Related to this: https://www.postgresql.org/message-id/18683-a98f79c0673be358%40postgresql.org
            # Helpful error message shown in front-end.
            Error.service(
              service: :replication,
              code: :publication_not_recognized,
              message:
                "Publication '#{state.publication_name}' is in an invalid state. You must drop and re-create the slot to use this publication with this slot."
            )

          is_exception(error) ->
            Exception.message(error)

          true ->
            inspect(error)
        end
      else
        {:ok, %{"active" => true} = _slot} ->
          "Replication slot '#{state.slot_name}' is currently in use by another connection"

        {:error, %Error.NotFoundError{entity: :replication_slot}} ->
          maybe_recreate_slot(state)
          "Replication slot '#{state.slot_name}' does not exist"

        {:error, error} ->
          Exception.message(error)
      end

    error =
      if is_binary(error_or_error_msg) do
        Error.service(service: :replication, message: error_or_error_msg)
      else
        error_or_error_msg
      end

    Health.put_event(
      :postgres_replication_slot,
      state.id,
      %Event{slug: :replication_connected, status: :fail, error: error}
    )

    :ok
  end

  defp maybe_recreate_slot(%SlotProducer.State{connect_opts: conn_opts} = state) do
    # Neon databases have ephemeral replication slots. At time of writing, this
    # happens after 45min of inactivity.
    # In the future, we will want to "rewind" the slot on create to last known good LSN
    if String.ends_with?(conn_opts[:hostname], ".aws.neon.tech") do
      CreateReplicationSlotWorker.enqueue(state.id)
    end
  end
end
