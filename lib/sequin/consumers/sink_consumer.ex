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
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SchemaFilter
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.SourceTable
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.TypesenseSink
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
             :health,
             :max_memory_mb,
             :legacy_transform,
             :timestamp_format,
             :batch_timeout_ms,
             :max_retry_count,
             :load_shedding_policy
           ]}
  typed_schema "sink_consumers" do
    field :name, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :max_retry_count, :integer, default: nil
    field :message_kind, Ecto.Enum, values: [:event, :record], default: :event
    field :status, Ecto.Enum, values: [:active, :disabled, :paused], default: :active
    field :seq, :integer, read_after_writes: true
    field :batch_size, :integer, default: 1
    field :batch_timeout_ms, :integer, default: nil
    field :annotations, :map, default: %{}
    field :max_memory_mb, :integer, default: 128
    field :partition_count, :integer, default: 1
    field :legacy_transform, Ecto.Enum, values: [:none, :record_only], default: :none
    field :timestamp_format, Ecto.Enum, values: [:iso8601, :unix_microsecond], default: :iso8601
    field :load_shedding_policy, Ecto.Enum, values: [:pause_on_full, :discard_on_full], default: :pause_on_full

    field :type, Ecto.Enum,
      values: [
        :http_push,
        :sqs,
        :kinesis,
        :redis_stream,
        :redis_string,
        :kafka,
        :sequin_stream,
        :gcp_pubsub,
        :nats,
        :rabbitmq,
        :azure_event_hub,
        :typesense,
        :sns,
        :elasticsearch
      ],
      read_after_writes: true

    field :health, :map, virtual: true

    embeds_many :source_tables, SourceTable, on_replace: :delete
    has_many :active_backfills, Backfill, where: [state: :active]

    # Sequences
    # FIXME: Refactor / remove both of these?
    belongs_to :sequence, Sequence
    embeds_one :sequence_filter, SequenceFilter, on_replace: :delete
    embeds_one :schema_filter, SchemaFilter, on_replace: :delete

    belongs_to :account, Account
    belongs_to :replication_slot, PostgresReplicationSlot
    has_one :postgres_database, through: [:replication_slot, :postgres_database]

    belongs_to :transform, Function
    belongs_to :routing, Function
    belongs_to :filter, Function

    polymorphic_embeds_one(:sink,
      types: [
        http_push: HttpPushSink,
        sqs: SqsSink,
        kinesis: KinesisSink,
        sns: SnsSink,
        redis_stream: RedisStreamSink,
        redis_string: RedisStringSink,
        kafka: KafkaSink,
        sequin_stream: SequinStreamSink,
        gcp_pubsub: GcpPubsubSink,
        nats: NatsSink,
        rabbitmq: RabbitMqSink,
        azure_event_hub: AzureEventHubSink,
        typesense: TypesenseSink,
        elasticsearch: ElasticsearchSink
      ],
      on_replace: :update,
      type_field_name: :type
    )

    timestamps()
  end

  def create_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :replication_slot_id,
      :status,
      :sequence_id,
      :message_kind,
      :max_memory_mb,
      :transform_id,
      :routing_id,
      :filter_id
    ])
    |> changeset(attrs)
    |> cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
    |> cast_embed(:schema_filter, with: &SchemaFilter.create_changeset/2)
    |> foreign_key_constraint(:sequence_id)
    |> foreign_key_constraint(:transform_id)
    |> foreign_key_constraint(:routing_id)
    |> foreign_key_constraint(:filter_id)
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> check_constraint(:sequence_filter, name: "sequence_filter_check")
    |> check_constraint(:batch_size,
      name: "ensure_batch_size_one",
      message: "batch_size must be 1 when batch is false for webhook sinks"
    )
    |> validate_filter_constraints()
    |> Sequin.Changeset.validate_name()
  end

  def update_changeset(consumer, attrs) do
    consumer
    |> changeset(attrs)
    |> cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
    |> cast_embed(:schema_filter, with: &SchemaFilter.create_changeset/2)
    |> check_constraint(:batch_size,
      name: "ensure_batch_size_one",
      message: "batch_size must be 1 when batch is false for webhook sinks"
    )
    |> validate_filter_constraints()
  end

  def changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :name,
      :batch_size,
      :ack_wait_ms,
      :max_waiting,
      :max_ack_pending,
      :max_deliver,
      :max_retry_count,
      :backfill_completed_at,
      :status,
      :annotations,
      :max_memory_mb,
      :partition_count,
      :legacy_transform,
      :transform_id,
      :routing_id,
      :filter_id,
      :timestamp_format,
      :batch_timeout_ms,
      :load_shedding_policy
    ])
    |> cast_polymorphic_embed(:sink, required: true)
    |> Sequin.Changeset.cast_embed(:source_tables)
    |> put_defaults()
    |> validate_required([:name, :status, :replication_slot_id, :batch_size])
    |> validate_number(:ack_wait_ms, greater_than_or_equal_to: 500)
    |> validate_number(:batch_size, greater_than: 0)
    |> validate_number(:batch_size, less_than_or_equal_to: 1_000)
    |> validate_number(:batch_timeout_ms, greater_than: 0)
    |> validate_number(:max_memory_mb, greater_than_or_equal_to: 128)
    |> validate_number(:partition_count, greater_than_or_equal_to: 1)
    |> validate_number(:max_retry_count, greater_than: 0)
    |> validate_inclusion(:legacy_transform, [:none, :record_only])
    |> validate_routing(attrs)
    |> Sequin.Changeset.annotations_check_constraint()
  end

  defp validate_routing(cs, attrs) do
    case {attrs["routing_mode"] || attrs[:routing_mode], fetch_field(cs, :routing_id)} do
      {nil, {:data, _}} ->
        # Both unchanged, this is fine
        cs

      {"static", {_, nil}} ->
        cs

      {"static", {_, id}} when not is_nil(id) ->
        add_error(cs, :routing_id, "static routing cannot have linked router!")

      {"dynamic", {_, id}} when not is_nil(id) ->
        cs

      {"dynamic", {_, nil}} ->
        add_error(cs, :routing_id, "dynamic routing requires linked router!")

      zz ->
        add_error(cs, :routing_id, "unknown routing mode! #{inspect(zz)}")
    end
  end

  defp validate_filter_constraints(changeset) do
    sequence_filter = get_field(changeset, :sequence_filter)
    schema_filter = get_field(changeset, :schema_filter)

    cond do
      is_nil(sequence_filter) and is_nil(schema_filter) ->
        add_error(changeset, :base, "either sequence_filter or schema_filter must be set")

      not is_nil(sequence_filter) and not is_nil(schema_filter) ->
        add_error(changeset, :base, "cannot set both sequence_filter and schema_filter")

      true ->
        changeset
    end
  end

  defp put_defaults(changeset) do
    changeset
    |> put_change(:batch_size, get_field(changeset, :batch_size) || 1)
    |> put_change(:ack_wait_ms, get_field(changeset, :ack_wait_ms) || 30_000)
    |> put_change(:max_waiting, get_field(changeset, :max_waiting) || 20)
    |> put_change(:max_ack_pending, get_field(changeset, :max_ack_pending) || 10_000)
    |> put_change(:max_memory_mb, get_field(changeset, :max_memory_mb) || 128)
    |> put_change(:partition_count, get_field(changeset, :partition_count) || 1)
    |> put_change(:legacy_transform, get_field(changeset, :legacy_transform) || :none)
    |> put_change(:message_kind, get_field(changeset, :message_kind) || :event)
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

  def where_transform_id(query \\ base_query(), transform_id) do
    from([consumer: c] in query, where: c.transform_id == ^transform_id)
  end

  def where_any_function_id(query \\ base_query(), function_id) do
    from([consumer: c] in query,
      where: c.transform_id == ^function_id or c.routing_id == ^function_id or c.filter_id == ^function_id
    )
  end

  def where_type(query \\ base_query(), type) do
    from([consumer: c] in query, where: c.type == ^type)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer: c] in query, where: c.id == ^id)
  end

  def where_id_in(query \\ base_query(), ids) do
    from([consumer: c] in query, where: c.id in ^ids)
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
    if Sequin.String.uuid?(id_or_name) do
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

  def where_status_not(query \\ base_query(), status) do
    from([consumer: c] in query, where: c.status != ^status)
  end

  def join_postgres_database(query \\ base_query()) do
    from([consumer: c] in query,
      join: postgres_database in assoc(c, :postgres_database),
      as: :database
    )
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

  def preload_http_endpoint!(%HttpPushSink{http_endpoint: nil} = sink) do
    http_endpoint = Consumers.get_http_endpoint!(sink.http_endpoint_id)
    %{sink | http_endpoint: http_endpoint}
  end

  def preload_http_endpoint!(%SinkConsumer{sink: %HttpPushSink{http_endpoint: nil}} = consumer) do
    %{consumer | sink: preload_http_endpoint!(consumer.sink)}
  end

  def preload_http_endpoint!(consumer), do: consumer

  def preload_cached_http_endpoint(%HttpPushSink{http_endpoint: nil} = sink) do
    with {:ok, http_endpoint} <- Consumers.get_cached_http_endpoint(sink.http_endpoint_id) do
      {:ok, %{sink | http_endpoint: http_endpoint}}
    end
  end

  def preload_cached_http_endpoint(%SinkConsumer{sink: %HttpPushSink{http_endpoint: nil}} = consumer) do
    with {:ok, sink} <- preload_cached_http_endpoint(consumer.sink) do
      {:ok, %{consumer | sink: sink}}
    end
  end

  def preload_cached_http_endpoint(consumer), do: {:ok, consumer}
end
