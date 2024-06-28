defmodule Sequin.Streams.Message do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  @derive {Jason.Encoder, only: [:key, :stream_id, :data_hash, :data, :seq]}
  @primary_key false
  @schema_prefix "streams"
  typed_schema "messages" do
    field :key, :string, primary_key: true
    field :stream_id, Ecto.UUID, primary_key: true

    field :data_hash, :string
    field :data, :string
    field :seq, :integer

    field :ack_id, :string, virtual: true

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(message, attrs) do
    message
    |> cast(attrs, [:stream_id, :key, :data, :data_hash])
    |> validate_required([:stream_id, :key, :data, :data_hash])
  end

  def put_data_hash(msg) do
    Map.put(msg, :data_hash, Base.encode64(:crypto.hash(:sha256, msg.data)))
  end

  def where_key_and_stream_id(query \\ base_query(), key, stream_id) do
    from([message: m] in query, where: m.key == ^key and m.stream_id == ^stream_id)
  end

  def where_key_and_stream_id_in(query \\ base_query(), key_stream_id_pairs) do
    {keys, stream_ids} = Enum.unzip(key_stream_id_pairs)
    stream_ids = Enum.map(stream_ids, &UUID.string_to_binary!/1)

    from([message: m] in query,
      where: fragment("(?, ?) IN (SELECT UNNEST(?::text[]), UNNEST(?::uuid[]))", m.key, m.stream_id, ^keys, ^stream_ids)
    )
  end

  defp base_query(query \\ __MODULE__) do
    from(m in query, as: :message)
  end
end
