defmodule Sequin.IexHelpers do
  @moduledoc false
  alias Ecto.Repo
  alias Sequin.Consumers
  alias Sequin.ConsumersRuntime.Supervisor, as: ConsumerSupervisor
  alias Sequin.Databases
  alias Sequin.Repo

  @sinks_to_pipelines ConsumerSupervisor.sinks_to_pipelines()
  @sinks Map.keys(@sinks_to_pipelines)

  def via(:slot, id) do
    Sequin.DatabasesRuntime.SlotProcessor.via_tuple(id)
  end

  def via(:table_reader, id) do
    Sequin.DatabasesRuntime.TableReaderServer.via_tuple(id)
  end

  def via(:slot_store, id) do
    Sequin.DatabasesRuntime.SlotMessageStore.via_tuple(id)
  end

  def via(sink, id) when sink in @sinks do
    pipeline_mod = @sinks_to_pipelines[sink]
    pipeline_mod.via_tuple(id)
  end

  def whereis(:slot, pg_replication_or_database_id) do
    via = via(:slot, pg_replication_or_database_id)

    with nil <- GenServer.whereis(via) do
      # Might be a postgres database id
      case Databases.get_db(pg_replication_or_database_id) do
        {:ok, db} ->
          db = Repo.preload(db, :replication_slot)

          :slot
          |> via(db.replication_slot.id)
          |> GenServer.whereis()

        {:error, _} ->
          nil
      end
    end
  end

  def whereis(:table_reader, backfill_id_or_sink_consumer_id) do
    via = via(:table_reader, backfill_id_or_sink_consumer_id)

    with nil <- GenServer.whereis(via) do
      # Might be a sink consumer id
      case Consumers.get_consumer(backfill_id_or_sink_consumer_id) do
        {:ok, consumer} ->
          consumer = Repo.preload(consumer, :active_backfill)

          if consumer.active_backfill do
            :table_reader
            |> via(consumer.active_backfill.id)
            |> GenServer.whereis()
          end

        {:error, _} ->
          nil
      end
    end
  end

  def whereis(:slot_store, consumer_id) do
    :slot_store
    |> via(consumer_id)
    |> GenServer.whereis()
  end

  def whereis(:sink, id) do
    Enum.reduce_while(@sinks, nil, fn sink, acc ->
      case whereis(sink, id) do
        nil -> {:cont, acc}
        pid -> {:halt, pid}
      end
    end)
  end

  def whereis(entity, id) do
    entity |> via(id) |> GenServer.whereis()
  end

  def whereis(id) do
    with nil <- whereis(:slot, id),
         nil <- whereis(:table_reader, id),
         nil <- whereis(:slot_store, id) do
      whereis(:sink, id)
    end
  end
end
