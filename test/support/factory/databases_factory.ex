defmodule Sequin.Factory.DatabasesFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Repo

  # PostgresDatabase

  def postgres_database(attrs \\ []) do
    attrs = Map.new(attrs)

    {tables, attrs} = Map.pop_lazy(attrs, :tables, fn -> Enum.random([[table()], []]) end)

    {tables_refreshed_at, attrs} =
      Map.pop_lazy(attrs, :tables_refreshed_at, fn -> unless tables == [], do: Factory.timestamp() end)

    merge_attributes(
      %PostgresDatabase{
        id: Factory.uuid(),
        database: Factory.postgres_object(),
        hostname: Factory.hostname(),
        pool_size: Factory.integer(),
        port: Factory.port(),
        queue_interval: Factory.integer(),
        queue_target: Factory.integer(),
        name: Factory.name(),
        ssl: Factory.boolean(),
        username: Factory.username(),
        password: Factory.password(),
        tables: tables,
        tables_refreshed_at: tables_refreshed_at
      },
      attrs
    )
  end

  def postgres_database_attrs(attrs \\ []) do
    attrs
    |> postgres_database()
    |> Sequin.Map.from_ecto()
    |> Map.update(:tables, [], fn tables ->
      Enum.map(tables, fn table ->
        table |> Sequin.Map.from_ecto() |> table_attrs()
      end)
    end)
  end

  def insert_postgres_database!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs = postgres_database_attrs(attrs)

    %PostgresDatabase{account_id: account_id}
    |> PostgresDatabase.changeset(attrs)
    |> Repo.insert!()
  end

  def configured_postgres_database_attrs(attrs \\ []) do
    attrs = Map.new(attrs)

    Repo.config()
    |> Map.new()
    |> Map.take([
      :hostname,
      :port,
      :database,
      :username,
      :password,
      :pool_size
    ])
    |> Map.merge(attrs)
    # Set very low for tests, for any tests that are testing erroneous queries against a bad db connection
    |> Map.merge(%{queue_target: 50, queue_interval: 50})
    |> Map.put(:ssl, false)
    |> postgres_database_attrs()
  end

  def insert_configured_postgres_database!(attrs \\ []) do
    attrs
    |> configured_postgres_database_attrs()
    |> insert_postgres_database!()
  end

  def table(attrs \\ []) do
    merge_attributes(
      %PostgresDatabase.Table{
        schema: Factory.postgres_object(),
        name: Factory.postgres_object(),
        oid: Factory.unique_integer(),
        columns: [column()]
      },
      attrs
    )
  end

  def table_attrs(attrs \\ []) do
    attrs
    |> table()
    |> Sequin.Map.from_ecto()
    |> Map.update!(:columns, fn columns ->
      Enum.map(columns, &Sequin.Map.from_ecto/1)
    end)
  end

  def column(attrs \\ []) do
    merge_attributes(
      %PostgresDatabase.Table.Column{
        name: Factory.postgres_object(),
        type: Factory.one_of(["integer", "text", "boolean", "timestamp", "uuid"]),
        attnum: Factory.unique_integer(),
        is_pk?: Factory.boolean()
      },
      attrs
    )
  end

  def column_attrs(attrs \\ []) do
    attrs
    |> column()
    |> Sequin.Map.from_ecto()
  end
end
