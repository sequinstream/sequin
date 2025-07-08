defmodule Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage do
  @moduledoc false
  use TypedStruct

  alias __MODULE__
  alias Sequin.JSON

  @derive Jason.Encoder
  typedstruct do
    field :id, String.t()
    field :consumer_id, String.t()
    field :commit_lsn, String.t()
    field :commit_idx, non_neg_integer()
    field :ack_id, String.t()
    field :deliver_count, non_neg_integer()
    field :last_delivered_at, DateTime.t()
    field :seq, non_neg_integer()
    field :record_pks, list(String.t())
    field :table_oid, String.t()
    field :not_visible_until, DateTime.t()
    field :inserted_at, DateTime.t()
    field :commit_timestamp, DateTime.t()
    field :trace_id, String.t()
    field :table_name, String.t()
    field :table_schema, String.t()
    field :state, :discarded | :acknowledged
  end

  def encode(%AcknowledgedMessage{} = acknowledged_message) do
    acknowledged_message
    |> migrate_from_seq()
    |> JSON.encode_struct_with_type()
    |> Jason.encode!()
  end

  def decode(encoded_message) do
    encoded_message
    |> Jason.decode!()
    |> JSON.decode_struct_with_type()
    |> migrate_from_seq()
    |> ensure_delivered()
  end

  def from_json(json) do
    json
    |> JSON.decode_timestamp("last_delivered_at")
    |> JSON.decode_timestamp("inserted_at")
    |> JSON.decode_atom(:state)
    |> JSON.struct(AcknowledgedMessage)
  end

  # We're dual-writing commit_idx, commit_lsn, and seq. We're replacing seq with commit_lsn and commit_idx
  # We can safely drop seq from this data structure in the future.
  defp migrate_from_seq(%__MODULE__{} = message) do
    Map.update!(message, :commit_idx, fn commit_idx ->
      if is_nil(commit_idx) and not is_nil(message.seq) and not is_nil(message.commit_lsn) do
        message.seq - message.commit_lsn
      else
        commit_idx
      end
    end)
  end

  # Previously, if messages were delivered successfully on first attempt, the deliver_count was 0. It should show as `1` to avoid confusion.
  # Because we're now setting this to `1` on the way in to the AcknowledgedMessage store,
  # It should be safe to remove this line in the future.
  defp ensure_delivered(%__MODULE__{} = message) do
    if message.deliver_count == 0 do
      %{message | deliver_count: 1}
    else
      message
    end
  end
end
