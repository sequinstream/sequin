defmodule Sequin.Test.Support.Models.TestEventLog do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  alias Sequin.Postgres

  schema "test_event_logs" do
    field :seq, :integer
    field :source_database_id, Ecto.UUID
    field :source_oid, :integer
    field :source_pk, :string
    field :record, :map
    field :changes, :map
    field :action, :string
    field :committed_at, :utc_datetime_usec
    field :inserted_at, :utc_datetime_usec
  end

  def where_seq(query \\ base_query(), seq) do
    from(tel in query, where: tel.seq == ^seq)
  end

  def where_source_oid(query \\ base_query(), source_oid) do
    from(tel in query, where: tel.source_oid == ^source_oid)
  end

  def column_attnums do
    from(pg in "pg_attribute",
      where: pg.attrelid == ^table_oid() and pg.attnum > 0,
      select: {pg.attname, pg.attnum}
    )
    |> Sequin.Repo.all()
    |> Map.new()
  end

  def column_attnum(column_name) do
    Map.fetch!(column_attnums(), column_name)
  end

  def table_oid do
    Postgres.ecto_model_oid(__MODULE__)
  end

  defp base_query(query \\ __MODULE__) do
    from(tel in query, as: :test_event_log)
  end

  def changeset(test_event_log, attrs) do
    Ecto.Changeset.cast(test_event_log, attrs, [
      :seq,
      :source_database_id,
      :source_oid,
      :source_pk,
      :record,
      :changes,
      :action,
      :committed_at
    ])
  end
end
