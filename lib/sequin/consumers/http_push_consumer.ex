defmodule Sequin.Consumers.HttpPushConsumer do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Sequin.Accounts.Account
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SourceTable
  alias Sequin.Replication.PostgresReplicationSlot

  @derive {Jason.Encoder,
           only: [
             :account_id,
             :ack_wait_ms,
             :id,
             :inserted_at,
             :max_ack_pending,
             :max_deliver,
             :max_waiting,
             :message_kind,
             :name,
             :updated_at,
             :http_endpoint_id,
             :status,
             :health
           ]}
  typed_schema "http_push_consumers" do
    field :name, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :message_kind, Ecto.Enum, values: [:event, :record], default: :event
    field :status, Ecto.Enum, values: [:active, :disabled], default: :active
    field :seq, :integer, read_after_writes: true

    embeds_many :source_tables, SourceTable, on_replace: :delete

    belongs_to :account, Account
    belongs_to :replication_slot, PostgresReplicationSlot
    has_one :postgres_database, through: [:replication_slot, :postgres_database]

    belongs_to :http_endpoint, HttpEndpoint
    field :http_endpoint_path, :string

    field :health, :map, virtual: true

    timestamps()
  end

  def create_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :ack_wait_ms,
      :max_ack_pending,
      :max_deliver,
      :max_waiting,
      :message_kind,
      :name,
      :backfill_completed_at,
      :http_endpoint_id,
      :replication_slot_id,
      :http_endpoint_path,
      :status
    ])
    |> validate_required([:name, :status, :replication_slot_id])
    |> validate_number(:ack_wait_ms, greater_than_or_equal_to: 500)
    |> validate_http_endpoint_path()
    |> cast_assoc(:http_endpoint,
      with: fn _struct, attrs ->
        HttpEndpoint.create_changeset(%HttpEndpoint{account_id: consumer.account_id}, attrs)
      end
    )
    |> cast_embed(:source_tables)
    |> foreign_key_constraint(:http_endpoint_id)
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> Sequin.Changeset.validate_name()
  end

  def update_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :ack_wait_ms,
      :max_ack_pending,
      :max_deliver,
      :max_waiting,
      :backfill_completed_at,
      :http_endpoint_id,
      :status,
      :http_endpoint_path
    ])
    |> validate_number(:ack_wait_ms, greater_than_or_equal_to: 500)
    |> validate_http_endpoint_path()
    |> cast_embed(:source_tables)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([consumer: c] in query, where: c.account_id == ^account_id)
  end

  def where_replication_slot_id(query \\ base_query(), replication_slot_id) do
    from([consumer: c] in query, where: c.replication_slot_id == ^replication_slot_id)
  end

  def where_http_endpoint_id(query \\ base_query(), http_endpoint_id) do
    from([consumer: c] in query, where: c.http_endpoint_id == ^http_endpoint_id)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer: c] in query, where: c.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from([consumer: c] in query, where: c.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.is_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_kind(query \\ base_query(), kind) do
    from([consumer: c] in query, where: c.kind == ^kind)
  end

  def where_status(query \\ base_query(), status) do
    from([consumer: c] in query, where: c.status == ^status)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :consumer)
  end

  @backfill_completed_at_threshold :timer.minutes(5)
  def should_delete_acked_messages?(consumer, now \\ DateTime.utc_now())

  def should_delete_acked_messages?(%HttpPushConsumer{backfill_completed_at: nil}, _now), do: false

  def should_delete_acked_messages?(%HttpPushConsumer{backfill_completed_at: backfill_completed_at}, now) do
    backfill_completed_at
    |> DateTime.add(@backfill_completed_at_threshold, :millisecond)
    |> DateTime.compare(now) == :lt
  end

  defp validate_http_endpoint_path(changeset) do
    changeset
    |> validate_format(
      :http_endpoint_path,
      ~r/^\/[a-zA-Z0-9\-._~!$&'()*+,;=:@%\/]*$/,
      message: "must be a valid URL path"
    )
    |> validate_length(:http_endpoint_path, max: 2000)
  end
end
