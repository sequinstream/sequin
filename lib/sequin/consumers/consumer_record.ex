defmodule Sequin.Consumers.ConsumerRecord do
  @moduledoc false
  use Sequin.StreamSchema

  import Ecto.Changeset
  import Ecto.Query

  @primary_key false
  @derive {Jason.Encoder,
           only: [
             :consumer_id,
             :commit_lsn,
             :ack_id,
             :deliver_count,
             :last_delivered_at,
             :record_pks,
             :table_oid,
             :not_visible_until,
             :inserted_at
           ]}
  typed_schema "consumer_records" do
    field :consumer_id, Ecto.UUID, primary_key: true
    field :id, :integer, primary_key: true, read_after_writes: true
    field :commit_lsn, :integer
    field :record_pks, {:array, :string}
    field :table_oid, :integer
    field :state, Ecto.Enum, values: [:available, :acked, :delivered, :pending_redelivery], default: :available

    field :ack_id, Ecto.UUID, read_after_writes: true
    field :deliver_count, :integer
    field :last_delivered_at, :utc_datetime_usec
    field :not_visible_until, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end

  def create_changeset(consumer_record, attrs) do
    attrs = stringify_record_pks(attrs)

    consumer_record
    |> cast(attrs, [
      :consumer_id,
      :commit_lsn,
      :record_pks,
      :table_oid,
      :not_visible_until,
      :deliver_count,
      :last_delivered_at
    ])
    |> validate_required([
      :consumer_id,
      :record_pks,
      :table_oid,
      :deliver_count
    ])
  end

  def update_changeset(consumer_record, attrs) do
    cast(consumer_record, attrs, [:state, :commit_lsn, :last_delivered_at, :not_visible_until, :deliver_count])
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

    struct!(__MODULE__, attrs)
  end

  def where_consumer_id(query \\ base_query(), consumer_id) do
    from([consumer_record: cr] in query, where: cr.consumer_id == ^consumer_id)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer_record: cr] in query, where: cr.id == ^id)
  end

  def where_commit_lsn(query \\ base_query(), commit_lsn) do
    from([consumer_record: cr] in query, where: cr.commit_lsn == ^commit_lsn)
  end

  def where_commit_lsns(query \\ base_query(), commit_lsns) do
    from([consumer_record: cr] in query, where: cr.commit_lsn in ^commit_lsns)
  end

  def where_ack_ids(query \\ base_query(), ack_ids) do
    where(query, [consumer_record: cr], cr.ack_id in ^ack_ids)
  end

  def where_deliverable(query \\ base_query()) do
    now = DateTime.utc_now()

    from([consumer_record: cr] in query,
      where: is_nil(cr.not_visible_until) or cr.not_visible_until <= ^now
    )
  end

  def where_not_visible(query \\ base_query()) do
    now = DateTime.utc_now()

    from([consumer_record: cr] in query,
      where: not is_nil(cr.not_visible_until) and cr.not_visible_until > ^now
    )
  end

  def count(query \\ base_query()) do
    from([consumer_record: cr] in query, select: count(cr.id))
  end

  defp base_query(query \\ __MODULE__) do
    from(cr in query, as: :consumer_record)
  end
end
