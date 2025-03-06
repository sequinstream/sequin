defmodule Sequin.IexHelpers do
  @moduledoc false
  alias Ecto.Repo
  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Repo
  alias Sequin.Runtime.SlotMessageStoreSupervisor

  def via(:slot, id) do
    Sequin.Runtime.SlotProcessor.via_tuple(id)
  end

  def via(:table_reader, id) do
    Sequin.Runtime.TableReaderServer.via_tuple(id)
  end

  def via(:slot_stores, id) do
    with {:ok, consumer} <- Consumers.get_consumer(id) do
      sup_via = SlotMessageStoreSupervisor.via_tuple(consumer.id)

      store_vias =
        Enum.map(0..(consumer.partition_count - 1), fn partition ->
          Sequin.Runtime.SlotMessageStore.via_tuple(consumer.id, partition)
        end)

      {sup_via, store_vias}
    end
  end

  def via(:slot_store_sup, id) do
    Sequin.Runtime.SlotMessageStoreSupervisor.via_tuple(id)
  end

  def via(:sink, id) do
    Sequin.Runtime.SinkPipeline.via_tuple(id)
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

  def whereis(:slot_stores, consumer_id) do
    :slot_store_sup
    |> via(consumer_id)
    |> Supervisor.which_children()
    |> Enum.map(fn {_registered_name, pid, _type, _modules} -> pid end)
  end

  def whereis(:sink, id) do
    :sink
    |> via(id)
    |> GenServer.whereis()
  end

  def whereis(entity, id) do
    entity |> via(id) |> GenServer.whereis()
  end

  def whereis(id) do
    with nil <- whereis(:slot, id),
         nil <- whereis(:table_reader, id),
         nil <- whereis(:slot_stores, id) do
      whereis(:sink, id)
    end
  end

  def entity(id) do
    with nil <- Repo.get(Sequin.Accounts.Account, id),
         nil <- Repo.get(Sequin.Accounts.User, id),
         nil <- Repo.get(Sequin.Consumers.Backfill, id),
         nil <- Repo.get(Sequin.Consumers.SinkConsumer, id),
         nil <- Repo.get(Sequin.Databases.PostgresDatabase, id),
         nil <- Repo.get(Sequin.Replication.PostgresReplicationSlot, id),
         nil <- Repo.get(Sequin.Databases.Sequence, id) do
      Repo.get(Sequin.Replication.WalPipeline, id)
    end
  end

  # pids in datadog look like this:
  # #PID<0.30653.0>
  def dd_pid(pid) do
    [p1, p2, p3] =
      pid
      |> String.trim_leading("#PID<")
      |> String.trim_trailing(">")
      |> String.split(".")
      |> Enum.map(&String.to_integer/1)

    :c.pid(p1, p2, p3)
  end
end
