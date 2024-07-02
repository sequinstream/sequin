defmodule Sequin.Streams.Consumer do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Sequin.Accounts.Account
  alias Sequin.Streams.Stream

  @derive {Jason.Encoder,
           only: [
             :slug,
             :ack_wait_ms,
             :max_ack_pending,
             :max_deliver,
             :max_waiting,
             :stream_id,
             :account_id,
             :id,
             :inserted_at,
             :updated_at
           ]}
  typed_schema "consumers" do
    field :slug, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :filter_subject, :string

    belongs_to :stream, Stream
    belongs_to :account, Account

    timestamps()
  end

  def create_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :stream_id,
      :ack_wait_ms,
      :max_ack_pending,
      :max_deliver,
      :max_waiting,
      :slug,
      :filter_subject,
      :backfill_completed_at
    ])
    |> validate_required([:stream_id, :slug, :filter_subject])
    |> foreign_key_constraint(:stream_id)
  end

  def update_changeset(consumer, attrs) do
    cast(consumer, attrs, [:ack_wait_ms, :max_ack_pending, :max_deliver, :max_waiting, :backfill_completed_at])
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([consumer: c] in query, where: c.account_id == ^account_id)
  end

  def where_stream_id(query \\ base_query(), stream_id) do
    from([consumer: c] in query, where: c.stream_id == ^stream_id)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer: c] in query, where: c.id == ^id)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :consumer)
  end

  @doc """
  A consumer has a filter_subject field which is a `.` delimited string. Each token in the string is either a token that should be exactly matched, the `*` wildcard, or the `>` character. `>` can only come in the last position and indicates the filter_subject matches any trailing tokens. Without the `>` character, the filter_subject must exactly match in length to a message.subject.
  """
  def filter_matches_subject?(filter_subject, subject) do
    filter_tokens = String.split(filter_subject, ".")
    subject_tokens = String.split(subject, ".")

    cond do
      List.last(filter_tokens) == ">" ->
        match_with_trailing_wildcard(Enum.drop(filter_tokens, -1), subject_tokens)

      length(filter_tokens) != length(subject_tokens) ->
        false

      true ->
        match_tokens(filter_tokens, subject_tokens)
    end
  end

  defp match_with_trailing_wildcard(filter_tokens, subject_tokens) do
    length(subject_tokens) > length(filter_tokens) and
      match_tokens(filter_tokens, Enum.take(subject_tokens, length(filter_tokens)))
  end

  defp match_tokens(filter_tokens, subject_tokens) do
    filter_tokens
    |> Enum.zip(subject_tokens)
    |> Enum.all?(fn {filter_token, subject_token} ->
      filter_token == "*" or filter_token == subject_token
    end)
  end

  @backfill_completed_at_threshold :timer.minutes(5)
  def should_delete_acked_messages?(consumer, now \\ DateTime.utc_now())

  def should_delete_acked_messages?(%Consumer{backfill_completed_at: nil}, _now), do: false

  def should_delete_acked_messages?(%Consumer{backfill_completed_at: backfill_completed_at}, now) do
    backfill_completed_at
    |> DateTime.add(@backfill_completed_at_threshold, :millisecond)
    |> DateTime.compare(now) == :lt
  end
end
