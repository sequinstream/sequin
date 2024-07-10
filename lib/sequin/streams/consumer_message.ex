defmodule Sequin.Streams.ConsumerMessage do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Streams.Message

  @schema_prefix Application.compile_env(:sequin, [Sequin.Repo, :schema_prefix]) <> "streams"
  @primary_key false
  @derive {Jason.Encoder,
           only: [
             :consumer_id,
             :message_subject,
             :ack_id,
             :deliver_count,
             :last_delivered_at,
             :message_seq,
             :not_visible_until,
             :state
           ]}
  typed_schema "consumer_messages" do
    field :consumer_id, Ecto.UUID, primary_key: true
    field :message_subject, :string, primary_key: true

    field :ack_id, Ecto.UUID, read_after_writes: true
    field :deliver_count, :integer
    field :last_delivered_at, :utc_datetime_usec
    field :message_seq, :integer
    field :not_visible_until, :utc_datetime_usec
    field :state, Ecto.Enum, values: [:acked, :available, :delivered, :pending_redelivery]

    field :message, :map, virtual: true

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(consumer_message, attrs) do
    consumer_message
    |> cast(attrs, [
      :consumer_id,
      :message_seq,
      :message_subject,
      :state,
      :not_visible_until,
      :deliver_count,
      :last_delivered_at
    ])
    |> validate_required([:consumer_id, :message_seq, :message_subject, :state, :deliver_count])
  end

  def where_consumer_id(query \\ base_query(), consumer_id) do
    from([consumer_message: cm] in query, where: cm.consumer_id == ^consumer_id)
  end

  def where_message_subject(query \\ base_query(), message_subject) do
    from([consumer_message: cm] in query, where: cm.message_subject == ^message_subject)
  end

  def where_ack_ids(query \\ base_query(), ack_ids) do
    where(query, [consumer_message: cm], cm.ack_id in ^ack_ids)
  end

  def where_state(query \\ base_query(), state) do
    where(query, [consumer_message: cm], cm.state == ^state)
  end

  def where_state_not(query \\ base_query(), state) do
    where(query, [consumer_message: cm], cm.state != ^state)
  end

  def where_deliverable(query \\ base_query()) do
    now = DateTime.utc_now()

    from([consumer_message: cm] in query,
      where:
        cm.state == :available or
          (cm.state in [:delivered, :pending_redelivery] and cm.not_visible_until <= ^now)
    )
  end

  def join_message(query \\ base_query(), stream_id) do
    from [consumer_message: cm] in query,
      join: m in Sequin.Streams.Message,
      on: m.subject == cm.message_subject and m.stream_id == ^stream_id,
      as: :message
  end

  def where_subject_pattern(query \\ base_query(), pattern) do
    if has_named_binding?(query, :message) do
      Message.where_subject_pattern(query, pattern)
    else
      raise ArgumentError, "The query must have a joined message before calling where_subject_pattern/2"
    end
  end

  defp base_query(query \\ __MODULE__) do
    from(cm in query, as: :consumer_message)
  end
end
