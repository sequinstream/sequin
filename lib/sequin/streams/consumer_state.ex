defmodule Sequin.Streams.ConsumerState do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  @primary_key {:consumer_id, Ecto.UUID, autogenerate: false}
  @schema_prefix "streams"
  typed_schema "consumer_states" do
    field :message_seq_cursor, :integer, default: 0
    field :count_pulled_into_outstanding, :integer, default: 0

    timestamps()
  end

  def create_changeset(consumer_state, attrs) do
    consumer_state
    |> cast(attrs, [:consumer_id, :message_seq_cursor, :count_pulled_into_outstanding])
    |> validate_required([:consumer_id])
  end

  def where_consumer_id(query \\ base_query(), consumer_id) do
    from([consumer_state: cs] in query, where: cs.consumer_id == ^consumer_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :consumer_state)
  end
end
