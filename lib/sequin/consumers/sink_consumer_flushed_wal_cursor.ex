defmodule Sequin.Consumers.SinkConsumerFlushedWalCursor do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Consumers.SinkConsumer

  schema "sink_consumer_flushed_wal_cursors" do
    field :commit_lsn, :integer, default: 0
    field :commit_idx, :integer, default: 0

    belongs_to :sink_consumer, SinkConsumer

    timestamps()
  end

  def create_changeset(cursor, attrs) do
    cursor
    |> cast(attrs, [:commit_lsn, :commit_idx, :sink_consumer_id])
    |> validate_required([:commit_lsn, :commit_idx, :sink_consumer_id])
    |> foreign_key_constraint(:sink_consumer_id)
  end

  def update_changeset(cursor, attrs) do
    cursor
    |> cast(attrs, [:commit_lsn, :commit_idx])
    |> validate_required([:commit_lsn, :commit_idx])
    |> validate_commit_tuple_advance(cursor)
  end

  defp validate_commit_tuple_advance(changeset, %__MODULE__{} = cursor) do
    commit_lsn = cursor.commit_lsn
    commit_idx = cursor.commit_idx

    new_commit_lsn = get_field(changeset, :commit_lsn)
    new_commit_idx = get_field(changeset, :commit_idx)

    if new_commit_lsn < commit_lsn or (new_commit_lsn == commit_lsn and new_commit_idx < commit_idx) do
      add_error(
        changeset,
        :commit_idx,
        "Cannot move WAL cursor backwards. New values must be greater than or equal to old values."
      )
    else
      changeset
    end
  end
end
