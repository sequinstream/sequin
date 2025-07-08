defmodule Sequin.Factory.DatabasesFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
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

    tables =
      if tables == :character_tables do
        :character_tables
        |> :ets.lookup(:tables)
        |> List.first()
        |> elem(1)
      else
        tables
      end

    {tables_refreshed_at, attrs} =
      Map.pop_lazy(attrs, :tables_refreshed_at, fn -> if tables != [], do: Factory.timestamp() end)

    tables = maybe_cast_tables(tables)

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
        tables_refreshed_at: tables_refreshed_at,
        pg_major_version: Enum.random(12..17)
      },
      attrs
    )
  end

  defp maybe_cast_tables(tables) do
    Enum.map(tables, fn
      %PostgresDatabaseTable{} = table ->
        table

      table ->
        columns =
          Enum.map(table.columns, fn
            %PostgresDatabaseTable.Column{} = column ->
              column

            column ->
              column(column)
          end)

        table = table(table)

        %{table | columns: columns}
    end)
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

  def configured_postgres_database(attrs \\ []) do
    attrs
    |> configured_postgres_database_attrs()
    |> postgres_database()
  end

  def configured_postgres_database_attrs(attrs \\ []) do
    attrs =
      attrs
      |> Map.new()
      |> Map.put_new_lazy(:tables, fn ->
        :character_tables
        |> :ets.lookup(:tables)
        |> List.first()
        |> elem(1)
      end)

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
    # Set low for tests
    # If you want to test an erroneous connection, set queue_target and queue_interval to 1
    |> Map.merge(%{queue_target: 150, queue_interval: 1000, pool_size: 2})
    |> Map.merge(attrs)
    |> Map.put(:ssl, false)
    |> postgres_database_attrs()
  end

  def insert_configured_postgres_database!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs
    |> configured_postgres_database_attrs()
    |> insert_postgres_database!()
  end

  def table(attrs \\ []) do
    attrs = Map.new(attrs)

    {columns, attrs} =
      Map.pop_lazy(attrs, :columns, fn ->
        # Ensure at least one PK column
        pk = column(is_pk?: true, type: Enum.random(["integer", "uuid"]))
        num_columns = Enum.random(1..5)
        cols = for _ <- 1..num_columns, do: column()
        Enum.shuffle([pk | cols])
      end)

    merge_attributes(
      %PostgresDatabaseTable{
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

  def event_table(attrs \\ []) do
    attrs = Map.new(attrs)

    columns = [
      column_attrs(name: "id", type: "integer", is_pk?: true),
      column_attrs(name: "seq", type: "bigint"),
      column_attrs(name: "source_database_id", type: "uuid"),
      column_attrs(name: "source_table_oid", type: "bigint"),
      column_attrs(name: "source_table_schema", type: "text"),
      column_attrs(name: "source_table_name", type: "text"),
      column_attrs(name: "record_pk", type: "text"),
      column_attrs(name: "record", type: "jsonb"),
      column_attrs(name: "changes", type: "jsonb"),
      column_attrs(name: "action", type: "text"),
      column_attrs(name: "committed_at", type: "timestamp with time zone"),
      column_attrs(name: "inserted_at", type: "timestamp with time zone")
    ]

    merge_attributes(
      %PostgresDatabaseTable{
        schema: Map.get(attrs, :schema, "public"),
        name: Map.get(attrs, :name, "sequin_events"),
        oid: Factory.unique_integer(),
        columns: columns
      },
      attrs
    )
  end

  def column(attrs \\ []) do
    merge_attributes(
      %PostgresDatabaseTable.Column{
        name: Factory.postgres_object(),
        type: Factory.one_of(["integer", "text", "boolean", "timestamp", "uuid"]),
        attnum: Factory.unique_integer(),
        is_pk?: Factory.boolean(),
        pg_typtype: Factory.one_of(["b", "d", "c"])
      },
      attrs
    )
  end

  def column_attrs(attrs \\ []) do
    attrs
    |> column()
    |> Sequin.Map.from_ecto()
  end

  def postgres_database_primary(attrs \\ []) do
    merge_attributes(
      %Sequin.Databases.PostgresDatabasePrimary{
        database: Factory.postgres_object(),
        hostname: Factory.hostname(),
        port: Factory.port(),
        ssl: Factory.boolean(),
        username: Factory.username(),
        password: Factory.password(),
        ipv6: Factory.boolean()
      },
      attrs
    )
  end

  def postgres_database_primary_attrs(attrs \\ []) do
    attrs
    |> postgres_database_primary()
    |> Sequin.Map.from_ecto()
  end
end
