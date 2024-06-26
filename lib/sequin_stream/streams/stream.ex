defmodule SequinStream.Streams.Stream do
  use SequinStream.Schema

  import Ecto.Changeset

  alias SequinStream.Accounts.Account
  alias SequinStream.Streams.Stream

  typed_schema "streams" do
    field :idx, :integer, read_after_writes: true
    belongs_to :account, Account

    timestamps()
  end

  def changeset(%Stream{} = stream, attrs) do
    stream
    |> cast(attrs, [:account_id])
    |> validate_required([:account_id])
  end
end
