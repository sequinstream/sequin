defmodule Sequin.Streams.OutstandingMessage do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  @schema_prefix "streams"
  typed_schema "outstanding_messages" do
    field :consumer_id, Ecto.UUID
    field :deliver_count, :integer
    field :last_delivered_at, :utc_datetime_usec
    field :message_key, :string
    field :message_seq, :integer
    field :message_stream_id, Ecto.UUID
    field :not_visible_until, :utc_datetime_usec
    field :state, Ecto.Enum, values: [:delivered, :available, :pending_redelivery]

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(outstanding_message, attrs) do
    outstanding_message
    |> cast(attrs, [
      :consumer_id,
      :message_seq,
      :message_key,
      :message_stream_id,
      :state,
      :not_visible_until,
      :deliver_count,
      :last_delivered_at
    ])
    |> validate_required([:consumer_id, :message_seq, :message_key, :message_stream_id, :state, :deliver_count])
  end

  def where_consumer_id(query \\ base_query(), consumer_id) do
    from([outstanding_message: om] in query, where: om.consumer_id == ^consumer_id)
  end

  def where_deliverable(query \\ base_query()) do
    now = DateTime.utc_now()

    from([outstanding_message: om] in query,
      where:
        om.state == :available or
          (om.state in [:delivered, :pending_redelivery] and om.not_visible_until <= ^now)
    )
  end

  defp base_query(query \\ __MODULE__) do
    from(om in query, as: :outstanding_message)
  end
end
