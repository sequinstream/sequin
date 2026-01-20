defmodule Sequin.Tasks.MigratePostgresDbs do
  @moduledoc """
  One-off helper module for migrating sequences and sinks between PostgreSQL databases. For when the source db and target db have the same schema, but different table OIDs etc (different pg db instance).

  This module helps move sequence definitions from one database to another while
  preserving the table schema and name, but generating a new UUID for the sequence name.

  You can remove this file if it's causing issues.
  """

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Databases.Sequence
  alias Sequin.Replication
  alias Sequin.Repo

  @doc """
  Migrates sequences from a source PostgreSQL database to a target PostgreSQL database.

  ## Parameters

  - account_id: The account ID that owns both databases
  - source_db_id: The ID of the source PostgreSQL database
  - target_db_id: The ID of the target PostgreSQL database

  ## Returns

  A tuple containing:
  - The number of successfully migrated sequences
  - The number of failed migrations
  - A list of error details for any failed migrations
  """
  @spec migrate_sequences(String.t(), String.t(), String.t()) :: {integer, integer, list}
  def migrate_sequences(account_id, source_db_id, target_db_id) do
    # Get source and target databases
    with {:ok, source_db} <- Databases.get_db(source_db_id),
         {:ok, target_db} <- Databases.get_db(target_db_id),
         {:ok, _} <- validate_same_account(account_id, source_db, target_db),
         {:ok, _source_tables} <- Databases.tables(source_db),
         {:ok, target_tables} <- Databases.tables(target_db) do
      # Build a mapping of {schema, table_name} to table_oid for the target DB
      target_table_map = build_table_map(target_tables)

      # List sequences for the source database
      source_sequences =
        account_id
        |> Sequence.where_account()
        |> Sequence.where_postgres_database_id(source_db_id)
        |> Repo.all()

      # Create new sequences for each source sequence
      Enum.reduce(source_sequences, {0, 0, []}, fn sequence, {success, failure, errors} ->
        case migrate_sequence(account_id, sequence, target_db_id, target_table_map) do
          {:ok, _new_sequence} ->
            {success + 1, failure, errors}

          {:error, error} ->
            error_info = %{
              sequence_id: sequence.id,
              table_schema: sequence.table_schema,
              table_name: sequence.table_name,
              error: error
            }

            {success, failure + 1, [error_info | errors]}
        end
      end)
    else
      {:error, error} -> {0, 0, [error]}
    end
  end

  @doc """
  Migrates sink consumers from a source replication slot to a target replication slot.

  ## Parameters

  - account_id: The account ID that owns both replication slots
  - source_replication_slot_id: The ID of the source replication slot
  - target_replication_slot_id: The ID of the target replication slot

  ## Returns

  A tuple containing:
  - The number of successfully migrated sink consumers
  - The number of failed migrations
  - A list of error details for any failed migrations
  """
  @spec migrate_sink_consumers(String.t(), String.t(), String.t()) :: {integer, integer, list}
  def migrate_sink_consumers(account_id, source_replication_slot_id, target_replication_slot_id) do
    with {:ok, source_slot} <- Replication.get_pg_replication(source_replication_slot_id),
         {:ok, target_slot} <- Replication.get_pg_replication(target_replication_slot_id),
         :ok <- validate_replication_slots(account_id, source_slot, target_slot) do
      # Load the PostgreSQL databases associated with each replication slot
      # source_db = Repo.preload(source_slot, :postgres_database).postgres_database
      target_db = Repo.preload(target_slot, :postgres_database).postgres_database

      # List all sink consumers for the source replication slot
      source_sinks =
        source_replication_slot_id
        |> Consumers.list_consumers_for_replication_slot()
        |> Repo.preload([:sequence, :postgres_database])

      # Load all target sequences for quick lookup
      target_sequences =
        account_id
        |> Sequence.where_account()
        |> Sequence.where_postgres_database_id(target_db.id)
        |> Repo.all()

      # Create new sinks for each source sink
      Enum.reduce(source_sinks, {0, 0, []}, fn source_sink, {success, failure, errors} ->
        case migrate_sink_consumer(account_id, source_sink, target_slot, target_sequences) do
          {:ok, _new_sink} ->
            {success + 1, failure, errors}

          {:error, error} ->
            error_info = %{
              sink_id: source_sink.id,
              sink_name: source_sink.name,
              error: error
            }

            {success, failure + 1, [error_info | errors]}
        end
      end)
    else
      {:error, error} -> {0, 0, [error]}
    end
  end

  # Migrates a single sink consumer from source to target replication slot
  defp migrate_sink_consumer(account_id, source_sink, target_slot, target_sequences) do
    # Find corresponding sequence in target database
    target_sequence = find_matching_sequence(source_sink.sequence, target_sequences)

    case target_sequence do
      nil ->
        {:error,
         "Matching sequence not found for #{source_sink.sequence.table_schema}.#{source_sink.sequence.table_name}"}

      sequence ->
        # Create a copy of attributes for the new sink
        attrs = prepare_sink_attrs(source_sink, target_slot.id, sequence.id)

        # Create the new sink consumer with skip_lifecycle to avoid immediate activation
        Consumers.create_sink_consumer(account_id, attrs, skip_lifecycle: true)
    end
  end

  # Find a sequence in the target database that matches the schema and table name of the source sequence
  defp find_matching_sequence(source_sequence, target_sequences) do
    Enum.find(target_sequences, fn seq ->
      seq.table_schema == source_sequence.table_schema &&
        seq.table_name == source_sequence.table_name
    end)
  end

  # Prepare attributes for creating a new sink consumer
  defp prepare_sink_attrs(source_sink, target_replication_slot_id, target_sequence_id) do
    # Convert to map and remove fields we don't want to copy
    sink_attrs =
      source_sink
      |> Map.from_struct()
      |> Map.take([
        :name,
        :ack_wait_ms,
        :batch_size,
        :max_waiting,
        :max_ack_pending,
        :max_deliver,
        :status,
        :annotations,
        :max_memory_mb,
        :partition_count,
        :legacy_transform,
        :transform_id,
        :timestamp_format
      ])
      # Append "_copy" to name to avoid conflicts
      |> Map.put(:name, String.replace(source_sink.name, "supabase-", "neon-"))
      # Set new replication slot and sequence
      |> Map.put(:replication_slot_id, target_replication_slot_id)
      |> Map.put(:sequence_id, target_sequence_id)

    # Copy the sequence filter if it exists
    sink_attrs =
      if source_sink.sequence_filter do
        sequence_filter = Map.from_struct(source_sink.sequence_filter)
        Map.put(sink_attrs, :sequence_filter, sequence_filter)
      else
        sink_attrs
      end

    # Copy the sink configuration (e.g. http_push, sqs, etc.)
    if source_sink.sink do
      Map.put(sink_attrs, :sink, extract_sink_config(source_sink))
    else
      sink_attrs
    end
  end

  # Extract sink configuration from the source sink
  defp extract_sink_config(source_sink) do
    # The sink field is a polymorphic embed, so we need to extract it properly
    sink_map =
      source_sink.sink
      |> Map.from_struct()
      |> Map.drop([:__meta__, :__struct__])

    # Add the sink type from the parent consumer
    Map.put(sink_map, :type, source_sink.type)
  end

  # Validates that both replication slots belong to the same account
  defp validate_replication_slots(account_id, source_slot, target_slot) do
    cond do
      source_slot.account_id != account_id ->
        {:error, "Source replication slot does not belong to the specified account"}

      target_slot.account_id != account_id ->
        {:error, "Target replication slot does not belong to the specified account"}

      source_slot.id == target_slot.id ->
        {:error, "Source and target replication slots cannot be the same"}

      true ->
        :ok
    end
  end

  # Validates that both databases belong to the same account
  defp validate_same_account(account_id, source_db, target_db) do
    cond do
      source_db.account_id != account_id ->
        {:error, "Source database does not belong to the specified account"}

      target_db.account_id != account_id ->
        {:error, "Target database does not belong to the specified account"}

      true ->
        {:ok, :valid}
    end
  end

  # Builds a map of {schema, table_name} to table_oid
  defp build_table_map(tables) do
    Enum.reduce(tables, %{}, fn table, acc ->
      Map.put(acc, {table.schema, table.name}, table.oid)
    end)
  end

  # Migrates a single sequence from the source to the target database
  defp migrate_sequence(account_id, source_sequence, target_db_id, target_table_map) do
    # Find the corresponding table in the target database
    case Map.get(target_table_map, {source_sequence.table_schema, source_sequence.table_name}) do
      nil ->
        {:error, "Table #{source_sequence.table_schema}.#{source_sequence.table_name} not found in target database"}

      target_table_oid ->
        # Create a new sequence with:
        # - Same table_schema and table_name
        # - The table_oid from the target database
        # - Auto-generated name (UUID)
        # - Same sort column settings
        attrs = %{
          name: Ecto.UUID.generate(),
          postgres_database_id: target_db_id,
          table_schema: source_sequence.table_schema,
          table_name: source_sequence.table_name,
          table_oid: target_table_oid,
          sort_column_attnum: source_sequence.sort_column_attnum,
          sort_column_name: source_sequence.sort_column_name
        }

        Databases.create_sequence(account_id, attrs)
    end
  end
end
