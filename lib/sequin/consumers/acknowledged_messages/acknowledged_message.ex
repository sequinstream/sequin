defmodule Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage do
  @moduledoc false
  use TypedStruct

  alias __MODULE__
  alias Sequin.JSON

  @derive Jason.Encoder
  typedstruct enforce: true do
    field :id, String.t()
    field :consumer_id, String.t()
    field :commit_lsn, String.t()
    field :ack_id, String.t()
    field :deliver_count, non_neg_integer()
    field :last_delivered_at, DateTime.t()
    field :seq, non_neg_integer()
    field :record_pks, list(String.t())
    field :table_oid, String.t()
    field :not_visible_until, DateTime.t()
    field :inserted_at, DateTime.t()
    field :trace_id, String.t()
  end

  def encode(%AcknowledgedMessage{} = acknowledged_message) do
    acknowledged_message
    |> JSON.encode_struct_with_type()
    |> Jason.encode!()
  end

  def decode(encoded_message) do
    encoded_message
    |> Jason.decode!()
    |> JSON.decode_struct_with_type()
  end

  def from_json(json) do
    json
    |> JSON.decode_timestamp("last_delivered_at")
    |> JSON.decode_timestamp("inserted_at")
    |> JSON.struct(AcknowledgedMessage)
  end
end
