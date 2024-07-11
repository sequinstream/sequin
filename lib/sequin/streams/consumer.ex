defmodule Sequin.Streams.Consumer do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Sequin.Accounts.Account
  alias Sequin.Streams.Stream

  @derive {Jason.Encoder,
           only: [
             :account_id,
             :ack_wait_ms,
             :filter_subject_pattern,
             :id,
             :inserted_at,
             :max_ack_pending,
             :max_deliver,
             :max_waiting,
             :slug,
             :stream_id,
             :updated_at
           ]}
  typed_schema "consumers" do
    field :slug, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :filter_subject_pattern, :string

    belongs_to :stream, Stream
    belongs_to :account, Account

    timestamps()
  end

  def create_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :stream_id,
      :ack_wait_ms,
      :max_ack_pending,
      :max_deliver,
      :max_waiting,
      :slug,
      :filter_subject_pattern,
      :backfill_completed_at
    ])
    |> validate_required([:stream_id, :slug, :filter_subject_pattern])
    |> foreign_key_constraint(:stream_id)
  end

  def update_changeset(consumer, attrs) do
    cast(consumer, attrs, [:ack_wait_ms, :max_ack_pending, :max_deliver, :max_waiting, :backfill_completed_at])
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([consumer: c] in query, where: c.account_id == ^account_id)
  end

  def where_stream_id(query \\ base_query(), stream_id) do
    from([consumer: c] in query, where: c.stream_id == ^stream_id)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer: c] in query, where: c.id == ^id)
  end

  def where_slug(query \\ base_query(), slug) do
    from([consumer: c] in query, where: c.slug == ^slug)
  end

  def where_id_or_slug(query \\ base_query(), id_or_slug) do
    if Sequin.String.is_uuid?(id_or_slug) do
      where_id(query, id_or_slug)
    else
      where_slug(query, id_or_slug)
    end
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :consumer)
  end

  @backfill_completed_at_threshold :timer.minutes(5)
  def should_delete_acked_messages?(consumer, now \\ DateTime.utc_now())

  def should_delete_acked_messages?(%Consumer{backfill_completed_at: nil}, _now), do: false

  def should_delete_acked_messages?(%Consumer{backfill_completed_at: backfill_completed_at}, now) do
    backfill_completed_at
    |> DateTime.add(@backfill_completed_at_threshold, :millisecond)
    |> DateTime.compare(now) == :lt
  end
end
