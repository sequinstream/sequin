defmodule Sequin.Factory.DatabasesFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Repo

  # PostgresDatabase

  def postgres_database(attrs \\ []) do
    attrs = Map.new(attrs)

    {table_count, attrs} = Map.pop(attrs, :table_count)

    {tables, attrs} =
      Map.pop_lazy(attrs, :tables, fn ->
        if table_count && table_count > 0 do
          for _ <- 1..table_count, do: table()
        else
          Enum.random([[table()], []])
        end
      end)

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
    |> Map.merge(%{queue_target: 150, queue_interval: 1000, pool_size: 2})
    |> Map.put(:ssl, false)
    |> postgres_database_attrs()
  end

  def insert_configured_postgres_database!(attrs \\ []) do
    attrs
    |> configured_postgres_database_attrs()
    |> insert_postgres_database!()
  end

  def table(attrs \\ []) do
    attrs = Map.new(attrs)

    {columns, attrs} =
      Map.pop_lazy(attrs, :columns, fn ->
        # Ensure at least one PK column
        pk = column_attrs(is_pk?: true, type: Enum.random(["integer", "uuid"]))
        cols = for _ <- 2..Enum.random(1..5), do: column_attrs()
        Enum.shuffle([pk | cols])
      end)

    merge_attributes(
      %PostgresDatabase.Table{
        schema: Factory.postgres_object(),
        name: Factory.postgres_object(),
        oid: Factory.unique_integer(),
        columns: columns
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

  # Sequence

  def sequence(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Sequence{
        id: Factory.uuid(),
        table_oid: Factory.unique_integer(),
        table_schema: Factory.postgres_object(),
        table_name: Factory.postgres_object(),
        sort_column_attnum: Factory.integer(),
        sort_column_name: Factory.postgres_object(),
        postgres_database_id: Factory.uuid()
      },
      attrs
    )
  end

  def sequence_attrs(attrs \\ []) do
    attrs
    |> sequence()
    |> Sequin.Map.from_ecto()
  end

  def insert_sequence!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs = Map.put_new_lazy(attrs, :postgres_database_id, fn -> insert_postgres_database!().id end)

    attrs = sequence_attrs(attrs)

    %Sequence{}
    |> Sequence.changeset(attrs)
    |> Repo.insert!()
  end
end
