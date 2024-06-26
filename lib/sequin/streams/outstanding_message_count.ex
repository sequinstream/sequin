defmodule Sequin.Streams.OutstandingMessageCount do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  typed_schema "outstanding_messages_count" do
    field :consumer_id, :string
    field :count, :integer
  end

  def changeset(outstanding_message_count, attrs) do
    outstanding_message_count
    |> cast(attrs, [:consumer_id, :count])
    |> validate_required([:consumer_id, :count])
  end
end
