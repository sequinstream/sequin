if Mix.env() != :prod do
  defmodule Sequin.Havoc do
    @moduledoc false

    alias Sequin.Consumers
    alias Sequin.Databases
    alias Sequin.Replication
    alias Sequin.Runtime.SinkPipeline
    alias Sequin.Runtime.SlotMessageStore
    alias Sequin.Runtime.SlotProducer.Processor

    require Logger

    @table_name :havoc_tasks

    @doc """
    Starts a task that randomly stops processes in the system at specified intervals.

    ## Arguments
      * `id` - A postgres database or replication slot ID
      * `opts` - Options for configuring the havoc
        * `:consumer_ids` - List of consumer IDs to target (defaults to all consumers for the slot)
        * `:interval` - Time in milliseconds between process stops (defaults to 5000)

    ## Examples
        iex> Sequin.Havoc.wreak("db_id_or_slot_id")
        iex> Sequin.Havoc.wreak("db_id_or_slot_id", consumer_ids: ["consumer_id"], interval: 10_000)
    """
    def wreak(id, opts \\ []) do
      # Try to get replication slot first
      slot_id =
        case Replication.get_pg_replication(id) do
          {:ok, slot} ->
            slot.id

          {:error, _} ->
            # Try to get database
            case Databases.get_db(id) do
              {:ok, db} ->
                db = Sequin.Repo.preload(db, :replication_slot)
                db.replication_slot.id

              error ->
                error
            end
        end

      # Get consumers if not provided
      consumers =
        case Keyword.get(opts, :consumer_ids, []) do
          [] ->
            Enum.filter(Consumers.all_consumers(), fn consumer -> consumer.replication_slot_id == slot_id end)

          consumer_ids ->
            Enum.map(consumer_ids, &Consumers.get_consumer!/1)
        end

      consumers = Enum.filter(consumers, &(&1.status != :disabled))

      interval = Keyword.get(opts, :interval, 5000)

      # Initialize ETS table if it doesn't exist
      if :ets.info(@table_name) == :undefined do
        :ets.new(@table_name, [:set, :named_table, :public])
      end

      # Start the havoc task
      timeout = Keyword.get(opts, :timeout, to_timeout(minute: 10))
      task = Task.async(fn -> wreak_havoc(slot_id, consumers, interval, timeout) end)

      # Store the task PID in ETS
      :ets.insert(@table_name, {slot_id, task.pid})

      Logger.info(
        "Havoc started for slot_id=#{slot_id} with #{length(consumers)} consumers, interval=#{interval}ms, timeout=#{timeout}ms"
      )

      :ok
    end

    @doc """
    Stops the havoc task for the given ID.
    """
    def stop(id) do
      # Check if the table exists
      if :ets.info(@table_name) == :undefined do
        {:error, :not_running}
        # Try to find by database ID
      else
        case :ets.lookup(@table_name, id) do
          [{^id, pid}] ->
            Process.exit(pid, :normal)
            :ets.delete(@table_name, id)
            Logger.info("Havoc stopped for id=#{id}")
            :ok

          [] ->
            # Try to find by database ID
            case Databases.get_db(id) do
              {:ok, db} ->
                db = Sequin.Repo.preload(db, :replication_slot)
                stop(db.replication_slot.id)

              {:error, _} ->
                Logger.warning("Havoc not found for id=#{id}")
                {:error, :not_found}
            end
        end
      end
    end

    # Private functions

    defp wreak_havoc(slot_id, consumers, interval, timeout, started_at \\ DateTime.utc_now()) do
      # Randomly select a process type to stop
      process_type =
        Enum.random([
          :slot_processor,
          :slot_message_store,
          :sink_pipeline,
          :consumer_producer,
          :processor,
          :reorder_buffer
        ])

      # Get a PID to stop based on the process type
      case get_random_pid(process_type, slot_id, consumers) do
        nil ->
          Logger.debug("Havoc: No #{process_type} process found to stop")
          :ok

        {pid, context} ->
          # Log what we're stopping with detailed information
          log_process_kill(process_type, pid, context)
          # Stop the process
          try do
            GenServer.stop(pid)
          catch
            e ->
              Logger.error("Error stopping process: #{inspect(e)}")
          end
      end

      # Sleep for the interval
      Process.sleep(interval)

      # Recurse to continue the havoc
      if !DateTime.after?(DateTime.utc_now(), DateTime.add(started_at, timeout, :millisecond)) do
        wreak_havoc(slot_id, consumers, interval, timeout, started_at)
      end
    end

    defp log_process_kill(process_type, pid, context) do
      message = "Havoc: Stopping #{process_type} process with PID #{inspect(pid)}"

      details =
        case {process_type, context} do
          {:slot_processor, slot_id} ->
            "slot_id=#{slot_id}"

          {:slot_message_store, {consumer_id, partition_idx}} ->
            "consumer_id=#{consumer_id}, partition=#{partition_idx}"

          {:sink_pipeline, consumer_id} ->
            "consumer_id=#{consumer_id}"

          {:consumer_producer, consumer_id} ->
            "consumer_id=#{consumer_id}"

          {:processor, {consumer_id, partition_idx}} ->
            "consumer_id=#{consumer_id}, partition=#{partition_idx}"

          {:reorder_buffer, slot_id} ->
            "slot_id=#{slot_id}"

          _ ->
            ""
        end

      full_message = if details == "", do: message, else: "#{message} (#{details})"

      IO.puts(full_message)
      Logger.info(full_message)
    end

    defp get_random_pid(:slot_processor, slot_id, _consumers) do
      case Sequin.IexHelpers.whereis(:slot, slot_id) do
        nil -> nil
        pid -> {pid, slot_id}
      end
    end

    defp get_random_pid(:slot_message_store, _slot_id, consumers) do
      if Enum.empty?(consumers) do
        nil
      else
        # Randomly select a consumer
        consumer = Enum.random(consumers)

        # Randomly select a partition
        partition_idx = :rand.uniform(consumer.partition_count) - 1

        # Get the store PID
        pid =
          consumer.id
          |> SlotMessageStore.via_tuple(partition_idx)
          |> GenServer.whereis()

        if pid, do: {pid, {consumer.id, partition_idx}}
      end
    end

    defp get_random_pid(:sink_pipeline, _slot_id, consumers) do
      if Enum.empty?(consumers) do
        nil
      else
        # Randomly select a consumer
        consumer = Enum.random(consumers)

        # Get the sink pipeline PID
        pid = Sequin.IexHelpers.whereis(:sink, consumer.id)

        if pid, do: {pid, consumer.id}
      end
    end

    defp get_random_pid(:consumer_producer, _slot_id, consumers) do
      if Enum.empty?(consumers) do
        nil
      else
        # Randomly select a consumer
        consumer = Enum.random(consumers)

        # Get the consumer producer PID
        pid =
          consumer.id
          |> SinkPipeline.producer()
          |> GenServer.whereis()

        if pid, do: {pid, consumer.id}
      end
    end

    defp get_random_pid(:processor, _slot_id, consumers) do
      if Enum.empty?(consumers) do
        nil
      else
        # Randomly select a consumer
        consumer = Enum.random(consumers)

        # Randomly select a partition
        partition_idx = :rand.uniform(Processor.partition_count()) - 1

        # Get the processor PID
        pid =
          consumer.id
          |> Processor.via_tuple(partition_idx)
          |> GenServer.whereis()

        if pid, do: {pid, {consumer.id, partition_idx}}
      end
    end

    defp get_random_pid(:reorder_buffer, slot_id, _consumers) do
      pid =
        slot_id
        |> Sequin.Runtime.SlotProducer.ReorderBuffer.via_tuple()
        |> GenServer.whereis()

      if pid, do: {pid, slot_id}
    end
  end
end
