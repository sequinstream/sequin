defmodule Sequin.Consumers.Backfill do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.SinkConsumer

  @derive {Jason.Encoder,
           only: [
             :id,
             :account_id,
             :sink_consumer_id,
             :state,
             :initial_min_cursor,
             :rows_initial_count,
             :rows_processed_count,
             :rows_ingested_count,
             :completed_at,
             :canceled_at,
             :inserted_at,
             :updated_at,
             :sort_column_attnum,
             :table_oid
           ]}
  typed_schema "backfills" do
    belongs_to :account, Account
    belongs_to :sink_consumer, SinkConsumer

    field :initial_min_cursor, Sequin.Ecto.IntegerKeyMap

    field :state, Ecto.Enum,
      values: [:active, :completed, :cancelled],
      default: :active

    field :rows_initial_count, :integer
    field :rows_processed_count, :integer, default: 0
    field :rows_ingested_count, :integer, default: 0
    field :completed_at, :utc_datetime_usec, read_after_writes: true
    field :canceled_at, :utc_datetime_usec, read_after_writes: true
    field :sort_column_attnum, :integer
    field :table_oid, :integer

    timestamps()
  end

  def create_changeset(backfill, attrs) do
    backfill
    |> cast(attrs, [
      :account_id,
      :sink_consumer_id,
      :state,
      :rows_initial_count,
      :initial_min_cursor,
      :sort_column_attnum,
      :table_oid
    ])
    |> validate_required([:account_id, :sink_consumer_id, :state, :initial_min_cursor, :table_oid])
    |> foreign_key_constraint(:sink_consumer_id)
    |> unique_constraint(:sink_consumer_id,
      name: "backfills_sink_consumer_id_index",
      message: "already has an active backfill"
    )
  end

  def update_changeset(backfill, attrs) do
    backfill
    |> cast(attrs, [
      :state,
      :rows_initial_count,
      :rows_processed_count,
      :rows_ingested_count
    ])
    |> validate_required([:state])
    |> validate_number(:rows_processed_count, greater_than_or_equal_to: 0)
    |> validate_number(:rows_ingested_count, greater_than_or_equal_to: 0)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([backfill: b] in query, where: b.account_id == ^account_id)
  end

  def where_sink_consumer_id(query \\ base_query(), sink_consumer_id) do
    from([backfill: b] in query, where: b.sink_consumer_id == ^sink_consumer_id)
  end

  def where_id(query \\ base_query(), id) do
    from([backfill: b] in query, where: b.id == ^id)
  end

  def where_state(query \\ base_query(), state) do
    from([backfill: b] in query, where: b.state == ^state)
  end

  defp base_query(query \\ __MODULE__) do
    from(b in query, as: :backfill)
  end
end
