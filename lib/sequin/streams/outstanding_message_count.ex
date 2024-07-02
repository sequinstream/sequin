defmodule Sequin.Streams.ConsumerMessageCount do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  typed_schema "consumer_messages_count" do
    field :consumer_id, :string
    field :count, :integer
  end

  def changeset(consumer_message_count, attrs) do
    consumer_message_count
    |> cast(attrs, [:consumer_id, :count])
    |> validate_required([:consumer_id, :count])
  end
end
