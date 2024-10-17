# lib/mix/tasks/sequin.migrate_to_sequences.ex
defmodule Mix.Tasks.Sequin.MigrateToSequences do
  @shortdoc "Migrate HTTP consumers to use Sequences instead of SourceTables"

  @moduledoc false
  use Mix.Task

  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Databases
  alias Sequin.Databases.Sequence
  alias Sequin.Repo

  require Logger

  def run(_args) do
    # Mix.Task.run("app.start")
    Application.ensure_all_started(:sequin)

    try do
      # required for migrations but doesn't seem to start when running them via `mix ecto.migrate`
      # in tests it's started as expected
      Sequin.Vault.start_link()
    rescue
      # if already start then just ignore the error
      _ -> nil
    end

    migrate_consumers()
  end

  def migrate_consumers do
    Repo.transaction(fn ->
      migrate_http_pull_consumers()
      migrate_http_push_consumers()
    end)
  end

  defp migrate_http_pull_consumers do
    HttpPullConsumer
    |> Repo.all()
    |> Repo.preload(replication_slot: [:postgres_database])
    |> Enum.each(&migrate_consumer(&1, HttpPullConsumer))
  end

  defp migrate_http_push_consumers do
    HttpPushConsumer
    |> Repo.all()
    |> Repo.preload(replication_slot: [:postgres_database])
    |> Enum.each(&migrate_consumer(&1, HttpPushConsumer))
  end

  defp migrate_consumer(consumer, consumer_module) do
    Logger.info("Migrating consumer #{consumer.id} of type #{consumer_module}",
      consumer: consumer,
      source_tables: consumer.source_tables
    )

    case consumer.source_tables do
      [source_table | _] ->
        sequence = fetch_sequence(consumer, source_table) || create_sequence(consumer, source_table)
        if sequence, do: update_consumer(consumer, consumer_module, sequence)

      _ ->
        Logger.info("Skipping consumer #{consumer.id} - no source table found")
    end
  end

  defp fetch_sequence(consumer, source_table) do
    Repo.get_by(Sequence,
      postgres_database_id: consumer.replication_slot.postgres_database_id,
      table_oid: source_table.oid
    )
  end

  defp create_sequence(consumer, source_table) do
    {:ok, tables} = Databases.tables(consumer.replication_slot.postgres_database)

    with table when not is_nil(table) <- Enum.find(tables, fn t -> t.oid == source_table.oid end),
         sort_column when not is_nil(sort_column) <-
           Enum.find(table.columns, fn c -> c.is_pk? end) || List.first(table.columns) do
      attrs = %Sequence{
        postgres_database_id: consumer.replication_slot.postgres_database_id,
        table_oid: source_table.oid,
        table_schema: table.schema,
        table_name: table.name,
        sort_column_attnum: sort_column.attnum,
        sort_column_name: sort_column.name
      }

      attrs = Map.from_struct(attrs)

      {:ok, sequence} = %Sequence{} |> Sequence.changeset(attrs) |> Repo.insert()
      sequence
    else
      _ ->
        Logger.warning(
          "No table found for consumer #{consumer.id} - source table #{source_table.oid}",
          tables: tables
        )

        nil
    end
  end

  defp update_consumer(consumer, consumer_module, sequence) do
    [source_table | _] = consumer.source_tables

    column_filters =
      Enum.map(source_table.column_filters, fn f ->
        f
        |> SequenceFilter.ColumnFilter.to_external()
        |> SequenceFilter.ColumnFilter.from_external()
      end)

    sequence_filter = %SequenceFilter{
      actions: source_table.actions,
      column_filters: column_filters,
      group_column_attnums: source_table.group_column_attnums
    }

    attrs = %{
      sequence_filter: Map.from_struct(sequence_filter),
      source_tables: []
    }

    {:ok, updated_consumer} =
      consumer
      |> consumer_module.update_changeset(attrs)
      # Not allowed by update_changeset
      |> Ecto.Changeset.put_change(:sequence_id, sequence.id)
      |> Repo.update()

    IO.puts("Updated #{consumer_module} #{updated_consumer.id}")
  end
end
