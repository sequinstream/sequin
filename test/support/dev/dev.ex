defmodule Sequin.Dev do
  @moduledoc false
  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Error.ValidationError
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Repo

  def setup(webhook_url) do
    fn ->
      account = AccountsFactory.insert_account!()
      database = create_database(account.id)
      http_endpoint = create_http_endpoint(account.id, webhook_url)

      with {:ok, replication_slot} <- create_replication_slot(database),
           {:ok, consumer} <- create_consumer(account.id, database, replication_slot, http_endpoint) do
        IO.puts("\nDatabase setup complete!")
        IO.puts("Account ID: #{account.id}")
        IO.puts("Database ID: #{database.id}")
        IO.puts("Replication Slot ID: #{replication_slot.id}")
        IO.puts("HTTP Endpoint ID: #{http_endpoint.id}")
        IO.puts("Consumer ID: #{consumer.id}")
        IO.puts("\nYour records will be streamed to: #{webhook_url}")

        {account, database, replication_slot, http_endpoint, consumer}
      else
        {:error, error} ->
          Repo.rollback(error)
      end
    end
    |> Repo.transaction()
    |> case do
      {:ok, _result} ->
        :ok

      {:error, error} ->
        IO.puts("\nSetup failed: #{inspect(error)}")
        :error
    end
  end

  defp create_database(account_id) do
    attrs =
      DatabasesFactory.configured_postgres_database_attrs(
        queue_target: 100,
        queue_interval: 1000,
        port: 5432,
        pool_size: 5,
        database: "sequin_dev_rep"
      )

    with {:ok, database} <- Databases.create_db_for_account(account_id, attrs),
         {:ok, database} <- Databases.update_tables(database) do
      database
    else
      {:error, %DBConnection.ConnectionError{}} ->
        IO.puts("\nError: Unable to connect to the database. Please ensure the database exists.")
        IO.puts("To create the database, run the following command:")
        IO.puts("createdb -h #{attrs.hostname} -p #{attrs.port} -U #{attrs.username} #{attrs.database}")
        IO.puts("\nAfter creating the database, please run the setup command again.")
        exit(:normal)

      {:error, error} ->
        IO.puts("\nAn unexpected error occurred: #{inspect(error)}")
        exit(:normal)
    end
  end

  defp create_replication_slot(database) do
    attrs =
      ReplicationFactory.postgres_replication_attrs(
        account_id: database.account_id,
        postgres_database_id: database.id,
        slot_name: "dev_replication_slot",
        publication_name: "dev_replication_publication"
      )

    case Replication.create_pg_replication_for_account_with_lifecycle(database.account_id, attrs) do
      {:ok, replication_slot} ->
        {:ok, replication_slot}

      {:error, %ValidationError{summary: "Replication slot `dev_replication_slot` does not exist"}} ->
        print_replication_setup_instructions(database, attrs.slot_name, attrs.publication_name)

      {:error, %ValidationError{summary: "Publication `dev_replication_publication` does not exist"}} ->
        print_replication_setup_instructions(database, attrs.slot_name, attrs.publication_name)

      {:error, error} ->
        IO.puts("\nAn unexpected error occurred: #{inspect(error)}")
        exit(:normal)
    end
  end

  defp print_replication_setup_instructions(database, slot_name, publication_name) do
    IO.puts("\nError: The replication slot or publication does not exist.")
    IO.puts("To set up replication, run the following commands:")
    IO.puts("   psql -h #{database.hostname} -p #{database.port} -U #{database.username} -d #{database.database}")
    IO.puts("   SELECT pg_create_logical_replication_slot('#{slot_name}', 'pgoutput');")
    IO.puts("   CREATE PUBLICATION #{publication_name} FOR ALL TABLES;")
    IO.puts("\nAfter creating both the replication slot and publication, please run the setup command again.")
    exit(:normal)
  end

  defp create_http_endpoint(account_id, webhook_url) do
    attrs = %{
      name: "Test Endpoint",
      headers: %{"Content-Type" => "application/json"},
      base_url: webhook_url
    }

    {:ok, endpoint} = Consumers.create_http_endpoint_for_account(account_id, attrs)
    endpoint
  end

  defp create_consumer(account_id, database, replication_slot, http_endpoint) do
    case Enum.find(database.tables, &(&1.name == "dune")) do
      nil ->
        IO.puts("Table not found")
        IO.puts("To create a table, follow these steps:")
        IO.puts("1. Connect to your database:")
        IO.puts("   psql -h #{database.hostname} -p #{database.port} -U #{database.username} -d #{database.database}")
        IO.puts("\n2. Run the following command:")
        IO.puts("   CREATE TABLE dune (id SERIAL PRIMARY KEY, name TEXT, house TEXT);")
        {:error, "Table not found"}

      table ->
        case Enum.find(table.columns, &(&1.name == "house")) do
          nil ->
            IO.puts("Column not found")
            IO.puts("To create a column, run the following command:")
            IO.puts("ALTER TABLE dune ADD COLUMN house TEXT;")
            {:error, "Column not found"}

          column ->
            attrs = %{
              name: "test_consumer",
              status: :active,
              message_kind: :record,
              ack_wait_ms: 30_000,
              max_ack_pending: 10_000,
              max_deliver: 93,
              max_waiting: 20,
              replication_slot_id: replication_slot.id,
              source_tables: [
                %{
                  oid: table.oid,
                  actions: [:insert, :update, :delete],
                  column_filters: [
                    %{
                      value: %{
                        value: "atreides",
                        __type__: :string
                      },
                      operator: :==,
                      column_attnum: column.attnum
                    }
                  ]
                }
              ],
              http_endpoint_id: http_endpoint.id
            }

            Consumers.create_destination_consumer_for_account_with_lifecycle(account_id, attrs)
        end
    end
  end
end
