defmodule Sequin.Replication.WalEvent do
  @moduledoc false
  use Sequin.StreamSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Replication.WalPipeline

  @derive {Jason.Encoder,
           only: [
             :id,
             :wal_pipeline_id,
             :commit_lsn,
             :record_pks,
             :record,
             :changes,
             :action,
             :committed_at,
             :inserted_at
           ]}
  schema "wal_events" do
    field :action, Ecto.Enum, values: [:insert, :update, :delete]
    field :changes, :map
    field :commit_lsn, :integer
    field :committed_at, :utc_datetime_usec
    field :record_pks, {:array, :string}
    field :record, :map
    field :replication_message_trace_id, Ecto.UUID
    field :source_table_oid, :integer

    belongs_to :wal_pipeline, WalPipeline

    timestamps()
  end

  def create_changeset(wal_event, attrs) do
    attrs = stringify_record_pks(attrs)

    wal_event
    |> cast(attrs, [
      :wal_pipeline_id,
      :commit_lsn,
      :record_pks,
      :replication_message_trace_id,
      :source_table_oid,
      :record,
      :changes,
      :action,
      :committed_at
    ])
    |> validate_required([
      :wal_pipeline_id,
      :commit_lsn,
      :record_pks,
      :replication_message_trace_id,
      :source_table_oid,
      :record,
      :action,
      :committed_at
    ])
  end

  def stringify_record_pks(attrs) when is_map(attrs) do
    case attrs do
      %{"record_pks" => pks} ->
        %{attrs | "record_pks" => stringify_record_pks(pks)}

      %{record_pks: pks} ->
        %{attrs | record_pks: stringify_record_pks(pks)}

      _ ->
        attrs
    end
  end

  def stringify_record_pks(pks) when is_list(pks) do
    Enum.map(pks, &to_string/1)
  end

  def from_map(attrs) do
    attrs
    |> Sequin.Map.atomize_keys()
    |> Map.update!(:record_pks, &stringify_record_pks/1)
    |> then(&struct!(__MODULE__, &1))
  end

  def where_source_table_oid(query \\ base_query(), source_table_oid) do
    from([wal_event: we] in query, where: we.source_table_oid == ^source_table_oid)
  end

  def where_commit_lsn(query \\ base_query(), commit_lsn) do
    from([wal_event: we] in query, where: we.commit_lsn == ^commit_lsn)
  end

  def where_commit_lsns(query \\ base_query(), commit_lsns) do
    from([wal_event: we] in query, where: we.commit_lsn in ^commit_lsns)
  end

  def count(query \\ base_query()) do
    from([wal_event: we] in query, select: count(we.id))
  end

  def where_wal_pipeline_id(query \\ base_query(), wal_pipeline_id) do
    from([wal_event: we] in query, where: we.wal_pipeline_id == ^wal_pipeline_id)
  end

  def where_wal_pipeline_id_in(query \\ base_query(), wal_pipeline_ids) do
    from([wal_event: we] in query, where: we.wal_pipeline_id in ^wal_pipeline_ids)
  end

  defp base_query(query \\ __MODULE__) do
    from(we in query, as: :wal_event)
  end
end
