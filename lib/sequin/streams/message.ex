defmodule Sequin.Streams.Message do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  @primary_key false
  @schema_prefix "streams"
  typed_schema "messages" do
    field :key, :string, primary_key: true
    field :stream_id, Ecto.UUID, primary_key: true

    field :data_hash, :string
    field :data, :string
    field :seq, :integer

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(message, attrs) do
    message
    |> cast(attrs, [:stream_id, :key, :data, :data_hash])
    |> validate_required([:stream_id, :key, :data, :data_hash])
  end

  def where_key_and_stream(query \\ base_query(), key, stream_id) do
    from([message: m] in query, where: m.key == ^key and m.stream_id == ^stream_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(m in query, as: :message)
  end
end
