defmodule Sequin.Streams.ConsumerState do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  @primary_key {:consumer_id, Ecto.UUID, autogenerate: false}
  @schema_prefix "streams"
  typed_schema "consumer_states" do
    field :message_seq_cursor, :integer

    timestamps()
  end

  def changeset(consumer_state, attrs) do
    consumer_state
    |> cast(attrs, [:consumer_id, :message_seq_cursor])
    |> validate_required([:consumer_id, :message_seq_cursor])
  end
end
