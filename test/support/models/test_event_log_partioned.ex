defmodule Sequin.TestSupport.Models.TestEventLogPartitioned do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  alias Sequin.Postgres

  @primary_key false
  schema "test_event_logs_partitioned" do
    field :id, :integer, primary_key: true, read_after_writes: true
    field :seq, :integer
    field :source_database_id, Ecto.UUID
    field :source_table_oid, :integer
    field :source_table_schema, :string
    field :source_table_name, :string
    field :record_pk, :string
    field :record, :map
    field :changes, :map
    field :action, :string
    field :committed_at, :utc_datetime, primary_key: true
    field :inserted_at, :utc_datetime
  end

  def where_seq(query \\ base_query(), seq) do
    from(tel in query, where: tel.seq == ^seq)
  end

  def where_source_table_oid(query \\ base_query(), source_table_oid) do
    from(tel in query, where: tel.source_table_oid == ^source_table_oid)
  end

  def table_name do
    "test_event_logs_partitioned"
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

  def pk_attnums do
    Enum.map(["id", "committed_at"], &column_attnum/1)
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
      :source_table_oid,
      :source_table_schema,
      :source_table_name,
      :record_pk,
      :record,
      :changes,
      :action,
      :committed_at
    ])
  end
end
