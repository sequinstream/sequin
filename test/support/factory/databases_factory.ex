defmodule Sequin.Factory.DatabasesFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Repo

  # PostgresDatabase

  def postgres_database(attrs \\ []) do
    merge_attributes(
      %PostgresDatabase{
        database: Factory.postgres_object(),
        hostname: Factory.hostname(),
        pool_size: Factory.integer(),
        port: Factory.port(),
        queue_interval: Factory.integer(),
        queue_target: Factory.integer(),
        slug: Factory.slug(),
        ssl: Factory.boolean(),
        username: Factory.username(),
        password: Factory.password(),
        account_id: Factory.uuid()
      },
      attrs
    )
  end

  def postgres_database_attrs(attrs \\ []) do
    attrs
    |> postgres_database()
    |> Sequin.Map.from_ecto()
  end

  def insert_postgres_database!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs = postgres_database_attrs(attrs)

    %PostgresDatabase{account_id: account_id}
    |> PostgresDatabase.changeset(attrs)
    |> Repo.insert!()
  end

  def insert_configured_postgres_database!(attrs \\ []) do
    attrs = Map.new(attrs)

    Repo.config()

    Repo.config()
    |> Map.new()
    |> Map.take([
      :hostname,
      :port,
      :database,
      :username,
      :password,
      :ssl,
      :queue_interval,
      :queue_target,
      :pool_size
    ])
    |> Map.merge(attrs)
    |> insert_postgres_database!()
  end
end
