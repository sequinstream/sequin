defmodule Sequin.Streams.Consumer do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Sequin.Accounts.Account
  alias Sequin.Streams.HttpEndpoint

  @derive {Jason.Encoder,
           only: [
             :account_id,
             :ack_wait_ms,
             :id,
             :inserted_at,
             :kind,
             :max_ack_pending,
             :max_deliver,
             :max_waiting,
             :message_kind,
             :name,
             :updated_at,
             :http_endpoint_id,
             :status
           ]}
  typed_schema "consumers" do
    field :name, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :message_kind, :string, default: "record"
    field :kind, Ecto.Enum, values: [:pull, :push], default: :pull
    field :status, Ecto.Enum, values: [:active, :disabled], default: :active

    belongs_to :account, Account
    belongs_to :http_endpoint, HttpEndpoint

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
      :kind,
      :http_endpoint_id,
      :status
    ])
    |> validate_required([:name, :kind, :status])
    |> cast_assoc(:http_endpoint,
      with: fn _struct, attrs ->
        HttpEndpoint.changeset(%HttpEndpoint{account_id: consumer.account_id}, attrs)
      end
    )
    |> foreign_key_constraint(:http_endpoint_id)
    |> Sequin.Changeset.validate_name()
    |> check_constraint(:kind, name: :kind_http_endpoint_constraint)
  end

  def update_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :ack_wait_ms,
      :max_ack_pending,
      :max_deliver,
      :max_waiting,
      :backfill_completed_at,
      :kind,
      :http_endpoint_id,
      :status
    ])
    |> check_constraint(:kind, name: :kind_http_endpoint_constraint)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([consumer: c] in query, where: c.account_id == ^account_id)
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

  def should_delete_acked_messages?(%Consumer{backfill_completed_at: nil}, _now), do: false

  def should_delete_acked_messages?(%Consumer{backfill_completed_at: backfill_completed_at}, now) do
    backfill_completed_at
    |> DateTime.add(@backfill_completed_at_threshold, :millisecond)
    |> DateTime.compare(now) == :lt
  end
end
