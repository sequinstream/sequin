defmodule Sequin.Consumers.ConsumerEvent do
  @moduledoc false
  use Sequin.StreamSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Consumers.ConsumerEventData

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
             :data,
             :inserted_at
           ]}
  typed_schema "consumer_events" do
    field :consumer_id, Ecto.UUID, primary_key: true
    field :id, :integer, primary_key: true, read_after_writes: true
    field :commit_lsn, :integer
    field :record_pks, {:array, :string}
    field :table_oid, :integer

    field :ack_id, Ecto.UUID, read_after_writes: true
    field :deliver_count, :integer
    field :last_delivered_at, :utc_datetime_usec
    field :not_visible_until, :utc_datetime_usec

    embeds_one :data, ConsumerEventData

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(consumer_event, attrs) do
    attrs = stringify_record_pks(attrs)

    consumer_event
    |> cast(attrs, [
      :consumer_id,
      :commit_lsn,
      :record_pks,
      :table_oid,
      :not_visible_until,
      :deliver_count,
      :last_delivered_at
    ])
    |> cast_embed(:data, required: true)
    |> validate_required([
      :consumer_id,
      :commit_lsn,
      :record_pks,
      :table_oid,
      :deliver_count,
      :data
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
        data = Map.put(data, :metadata, struct!(ConsumerEventData.Metadata, metadata))
        struct!(ConsumerEventData, data)
      end)

    struct!(__MODULE__, attrs)
  end

  def where_consumer_id(query \\ base_query(), consumer_id) do
    from([consumer_event: ce] in query, where: ce.consumer_id == ^consumer_id)
  end

  def where_commit_lsn(query \\ base_query(), commit_lsn) do
    from([consumer_event: ce] in query, where: ce.commit_lsn == ^commit_lsn)
  end

  def where_commit_lsns(query \\ base_query(), commit_lsns) do
    from([consumer_event: ce] in query, where: ce.commit_lsn in ^commit_lsns)
  end

  def where_ack_ids(query \\ base_query(), ack_ids) do
    where(query, [consumer_event: ce], ce.ack_id in ^ack_ids)
  end

  def where_deliverable(query \\ base_query()) do
    now = DateTime.utc_now()

    from([consumer_event: ce] in query,
      where: is_nil(ce.not_visible_until) or ce.not_visible_until <= ^now
    )
  end

  def where_not_visible(query \\ base_query()) do
    now = DateTime.utc_now()

    from([consumer_event: ce] in query,
      where: not is_nil(ce.not_visible_until) and ce.not_visible_until > ^now
    )
  end

  def count(query \\ base_query()) do
    from([consumer_event: ce] in query, select: count(ce.id))
  end

  defp base_query(query \\ __MODULE__) do
    from(ce in query, as: :consumer_event)
  end

  @spec external_state(%__MODULE__{}) :: :available | :pending
  def external_state(%__MODULE__{} = ce) do
    now = DateTime.utc_now()

    if is_nil(ce.not_visible_until) or DateTime.compare(ce.not_visible_until, now) != :gt do
      :available
    else
      :pending
    end
  end
end
