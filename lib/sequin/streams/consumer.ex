defmodule Sequin.Streams.Consumer do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  alias Sequin.Accounts.Account
  alias Sequin.Streams.Stream

  typed_schema "consumers" do
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20

    belongs_to :stream, Stream
    belongs_to :account, Account

    timestamps()
  end

  def changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [:stream_id, :account_id, :ack_wait_ms, :max_ack_pending, :max_deliver, :max_waiting])
    |> validate_required([:stream_id, :account_id])
  end
end
