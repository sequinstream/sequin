defmodule Sequin.Factory.SourcesFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Repo
  alias Sequin.Sources.PostgresReplication
  alias Sequin.Sources.Webhook

  def postgres_replication(attrs \\ []) do
    attrs = Map.new(attrs)

    {status, attrs} = Map.pop_lazy(attrs, :status, fn -> Factory.one_of([:active, :disabled, :backfilling]) end)
    status = if is_atom(status), do: status, else: String.to_atom(status)

    {backfill_completed_at, attrs} =
      Map.pop_lazy(attrs, :backfill_completed_at, fn ->
        if status in ["backfilling", :backfilling] do
          nil
        else
          Factory.timestamp()
        end
      end)

    merge_attributes(
      %PostgresReplication{
        backfill_completed_at: backfill_completed_at,
        postgres_database_id: Factory.uuid(),
        publication_name: "pub_#{Factory.name()}",
        slot_name: "slot_#{Factory.name()}",
        status: status,
        stream_id: Factory.uuid(),
        key_format: Factory.one_of([:basic, :with_operation])
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

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :postgres_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    {stream_id, attrs} =
      Map.pop_lazy(attrs, :stream_id, fn -> StreamsFactory.insert_stream!(account_id: account_id).id end)

    attrs
    |> Map.put(:account_id, account_id)
    |> Map.put(:postgres_database_id, postgres_database_id)
    |> Map.put(:stream_id, stream_id)
    |> postgres_replication()
    |> Repo.insert!()
  end

  def webhook(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Webhook{
        name: "webhook_#{Factory.name()}",
        stream_id: Factory.uuid(),
        account_id: Factory.uuid()
      },
      attrs
    )
  end

  def webhook_attrs(attrs \\ []) do
    attrs
    |> webhook()
    |> Sequin.Map.from_ecto()
  end

  def insert_webhook!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {stream_id, attrs} =
      Map.pop_lazy(attrs, :stream_id, fn -> StreamsFactory.insert_stream!(account_id: account_id).id end)

    attrs
    |> Map.put(:account_id, account_id)
    |> Map.put(:stream_id, stream_id)
    |> webhook()
    |> Repo.insert!()
  end

  def insert_stream!(attrs \\ []) do
    attrs = Map.new(attrs)
    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs
    |> Map.put(:account_id, account_id)
    |> StreamsFactory.stream()
    |> Repo.insert!()
  end
end
