defmodule Sequin.Consumers.SinkConsumer do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query
  import PolymorphicEmbed

  alias __MODULE__
  alias Sequin.Accounts.Account
  alias Sequin.Consumers
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumerFlushedWalCursor
  alias Sequin.Consumers.SourceTable
  alias Sequin.Consumers.SqsSink
  alias Sequin.Databases.Sequence
  alias Sequin.Replication.PostgresReplicationSlot

  @type id :: String.t()
  @type ack_id :: String.t()
  @type not_visible_until :: DateTime.t()

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
             :status,
             :health
           ]}
  typed_schema "sink_consumers" do
    field :name, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :message_kind, Ecto.Enum, values: [:event, :record], default: :event
    field :status, Ecto.Enum, values: [:active, :disabled, :paused], default: :active
    field :seq, :integer, read_after_writes: true
    field :batch_size, :integer, default: 1
    field :annotations, :map, default: %{}

    field :type, Ecto.Enum,
      values: [:http_push, :sqs, :redis, :kafka, :sequin_stream, :gcp_pubsub, :nats, :rabbitmq, :azure_event_hub],
      read_after_writes: true

    field :health, :map, virtual: true

    embeds_many :source_tables, SourceTable, on_replace: :delete
    has_one :active_backfill, Backfill, where: [state: :active]

    # Sequences
    belongs_to :sequence, Sequence
    embeds_one :sequence_filter, SequenceFilter, on_replace: :delete

    belongs_to :account, Account
    belongs_to :replication_slot, PostgresReplicationSlot
    has_one :postgres_database, through: [:replication_slot, :postgres_database]

    polymorphic_embeds_one(:sink,
      types: [
        http_push: HttpPushSink,
        sqs: SqsSink,
        redis: RedisSink,
        kafka: KafkaSink,
        sequin_stream: SequinStreamSink,
        gcp_pubsub: GcpPubsubSink,
        nats: NatsSink,
        rabbitmq: RabbitMqSink,
        azure_event_hub: AzureEventHubSink
      ],
      on_replace: :update,
      type_field_name: :type
    )

    has_one :flushed_wal_cursor, SinkConsumerFlushedWalCursor

    timestamps()
  end

  def create_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :name,
      :replication_slot_id,
      :status,
      :sequence_id,
      :message_kind
    ])
    |> changeset(attrs)
    |> cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
    |> foreign_key_constraint(:sequence_id)
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> check_constraint(:sequence_filter, name: "sequence_filter_check")
    |> Sequin.Changeset.validate_name()
  end

  def update_changeset(consumer, attrs) do
    consumer
    |> changeset(attrs)
    |> cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
  end

  def changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :batch_size,
      :ack_wait_ms,
      :max_waiting,
      :max_ack_pending,
      :max_deliver,
      :backfill_completed_at,
      :status,
      :annotations
    ])
    |> cast_polymorphic_embed(:sink, required: true)
    |> Sequin.Changeset.cast_embed(:source_tables)
    |> validate_required([:name, :status, :replication_slot_id, :batch_size])
    |> validate_number(:ack_wait_ms, greater_than_or_equal_to: 500)
    |> validate_number(:batch_size, greater_than: 0)
    |> validate_number(:batch_size, less_than_or_equal_to: 1_000)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([consumer: c] in query, where: c.account_id == ^account_id)
  end

  def where_replication_slot_id(query \\ base_query(), replication_slot_id) do
    from([consumer: c] in query, where: c.replication_slot_id == ^replication_slot_id)
  end

  def where_http_endpoint_id(query \\ base_query(), http_endpoint_id) do
    query = where_type(query, :http_push)

    from([consumer: c] in query, where: fragment("?->>'http_endpoint_id' = ?", c.sink, ^http_endpoint_id))
  end

  def where_type(query \\ base_query(), type) do
    from([consumer: c] in query, where: c.type == ^type)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer: c] in query, where: c.id == ^id)
  end

  def where_sequence_id(query \\ base_query(), sequence_id) do
    from([consumer: c] in query, where: c.sequence_id == ^sequence_id)
  end

  def where_active_backfill(query \\ base_query()) do
    from([consumer: c] in query,
      inner_join: b in Backfill,
      on: b.sink_consumer_id == c.id and b.state == :active
    )
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

  def should_delete_acked_messages?(%SinkConsumer{backfill_completed_at: nil}, _now), do: false

  def should_delete_acked_messages?(%SinkConsumer{backfill_completed_at: backfill_completed_at}, now) do
    backfill_completed_at
    |> DateTime.add(@backfill_completed_at_threshold, :millisecond)
    |> DateTime.compare(now) == :lt
  end

  def preload_http_endpoint(%SinkConsumer{sink: %HttpPushSink{http_endpoint: nil}} = consumer) do
    http_endpoint = Consumers.get_http_endpoint!(consumer.sink.http_endpoint_id)
    sink = %HttpPushSink{consumer.sink | http_endpoint: http_endpoint}
    %{consumer | sink: sink}
  end

  def preload_http_endpoint(consumer), do: consumer
end
