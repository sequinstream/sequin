defmodule Sequin.Replication.WalEvent do
  @moduledoc false
  use Sequin.StreamSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Replication.WalEventData
  alias Sequin.Replication.WalProjection

  @derive {Jason.Encoder,
           only: [
             :id,
             :wal_projection_id,
             :commit_lsn,
             :record_pks,
             :data,
             :inserted_at
           ]}
  schema "wal_events" do
    field :commit_lsn, :integer
    field :record_pks, {:array, :string}
    field :replication_message_trace_id, Ecto.UUID

    embeds_one :data, WalEventData
    belongs_to :wal_projection, WalProjection

    timestamps(type: :utc_datetime_usec)
  end

  def create_changeset(wal_event, attrs) do
    attrs = stringify_record_pks(attrs)

    wal_event
    |> cast(attrs, [
      :wal_projection_id,
      :commit_lsn,
      :record_pks,
      :replication_message_trace_id
    ])
    |> cast_embed(:data, required: true)
    |> validate_required([
      :wal_projection_id,
      :commit_lsn,
      :record_pks,
      :data,
      :replication_message_trace_id
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
    attrs =
      attrs
      |> Sequin.Map.atomize_keys()
      |> Map.update!(:record_pks, &stringify_record_pks/1)
      |> Map.update!(:data, fn data ->
        data = Sequin.Map.atomize_keys(data)
        metadata = Sequin.Map.atomize_keys(data.metadata)
        data = Map.put(data, :metadata, struct!(WalEventData.Metadata, metadata))
        struct!(WalEventData, data)
      end)

    struct!(__MODULE__, attrs)
  end

  def where_source_replication_slot_id(query \\ base_query(), source_replication_slot_id) do
    from([wal_event: we] in query, where: we.source_replication_slot_id == ^source_replication_slot_id)
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

  def where_wal_projection_id(query \\ base_query(), wal_projection_id) do
    from([wal_event: we] in query, where: we.wal_projection_id == ^wal_projection_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(we in query, as: :wal_event)
  end
end
