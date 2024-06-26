defmodule Sequin.Streams.Message do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  @primary_key false
  @schema_prefix "streams"
  typed_schema "messages" do
    field :key, Ecto.UUID, primary_key: true
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
end
