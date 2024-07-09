defmodule Sequin.Factory.SourcesFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Repo
  alias Sequin.Sources.PostgresReplication

  def postgres_replication(attrs \\ []) do
    merge_attributes(
      %PostgresReplication{
        slot_name: "slot_#{Factory.slug()}",
        publication_name: "pub_#{Factory.slug()}",
        status: Factory.one_of(["active", "disabled"]),
        postgres_database_id: Factory.uuid(),
        stream_id: Factory.uuid()
      },
      attrs
    )
  end

  def postgres_replication_attrs(attrs \\ []) do
    attrs
    |> postgres_replication()
    |> Sequin.Map.from_ecto()
  end

  def insert_postgres_replication!(attrs \\ []) do
    attrs = Map.new(attrs)

    {postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :postgres_database_id, fn -> DatabasesFactory.insert_postgres_database!().id end)

    {stream_id, attrs} = Map.pop_lazy(attrs, :stream_id, fn -> StreamsFactory.insert_stream!().id end)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs =
      attrs
      |> Map.put(:postgres_database_id, postgres_database_id)
      |> Map.put(:stream_id, stream_id)
      |> postgres_replication_attrs()

    %PostgresReplication{account_id: account_id}
    |> PostgresReplication.create_changeset(attrs)
    |> Repo.insert!()
  end
end
