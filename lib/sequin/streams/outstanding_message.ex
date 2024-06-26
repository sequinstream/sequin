defmodule Sequin.Streams.OutstandingMessage do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  @schema_prefix "streams"
  typed_schema "outstanding_messages" do
    field :consumer_id, :string
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
end
