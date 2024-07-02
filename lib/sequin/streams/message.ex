defmodule Sequin.Streams.Message do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  @token_keys [
    :token1,
    :token2,
    :token3,
    :token4,
    :token5,
    :token6,
    :token7,
    :token8,
    :token9,
    :token10,
    :token11,
    :token12,
    :token13,
    :token14,
    :token15,
    :token16
  ]

  @derive {Jason.Encoder, only: [:subject, :stream_id, :data_hash, :data, :seq] ++ @token_keys}
  @primary_key false
  @schema_prefix "streams"
  typed_schema "messages" do
    field :subject, :string, primary_key: true, read_after_writes: true
    field :stream_id, Ecto.UUID, primary_key: true

    field :data_hash, :string
    field :data, :string
    field :seq, :integer, read_after_writes: true

    field :token1, :string
    field :token2, :string
    field :token3, :string
    field :token4, :string
    field :token5, :string
    field :token6, :string
    field :token7, :string
    field :token8, :string
    field :token9, :string
    field :token10, :string
    field :token11, :string
    field :token12, :string
    field :token13, :string
    field :token14, :string
    field :token15, :string
    field :token16, :string

    field :ack_id, :string, virtual: true

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(message, attrs) do
    message
    |> cast(attrs, [:stream_id, :subject, :data, :data_hash])
    |> validate_required([:stream_id, :subject, :data, :data_hash])
    |> validate_subject()
    |> put_tokens()
  end

  defp validate_subject(changeset) do
    subject = get_field(changeset, :subject)

    token_count = subject |> String.split(".") |> length()
    invalid_chars = ~r/[\s*>\/\\\+\{\}\(\)\~\#\@]/
    contains_invalid_chars? = String.match?(subject, invalid_chars)

    leading_delimiter? = String.starts_with?(subject, ".")
    trailing_delimiter? = String.ends_with?(subject, ".")
    empty_tokens? = subject |> String.split(".") |> Enum.any?(&(&1 == ""))

    cond do
      subject == "" ->
        add_error(changeset, :subject, "Invalid subject: must not be empty")

      token_count < 1 or token_count > 16 ->
        add_error(changeset, :subject, "Invalid subject: must contain 1 to 16 tokens")

      leading_delimiter? ->
        add_error(changeset, :subject, "Invalid subject: must not start with a delimiter")

      trailing_delimiter? ->
        add_error(changeset, :subject, "Invalid subject: must not end with a delimiter")

      empty_tokens? ->
        add_error(changeset, :subject, "Invalid subject: must not contain empty tokens")

      contains_invalid_chars? ->
        add_error(changeset, :subject, "Invalid subject: contains invalid characters")

      true ->
        changeset
    end
  end

  # set token1..token16 to the token components of the subject
  # and remove subject from the changeset
  def put_tokens(%Ecto.Changeset{valid?: true} = changeset) do
    subject = get_field(changeset, :subject)
    tokens = String.split(subject, ".")

    tokens
    |> Enum.with_index()
    |> Enum.reduce(changeset, fn {token, index}, acc ->
      put_change(acc, :"token#{index + 1}", token)
    end)
    |> put_change(:subject, nil)
  end

  def put_tokens(%Ecto.Changeset{} = changeset), do: changeset

  def put_tokens(msg) when is_map(msg) do
    subject = msg.subject

    tokens = String.split(subject, ".")

    tokens
    |> Enum.with_index()
    |> Enum.reduce(msg, fn {token, index}, acc ->
      Map.put(acc, :"token#{index + 1}", token)
    end)
    |> Map.delete(:subject)
  end

  def put_data_hash(msg) do
    Map.put(msg, :data_hash, Base.encode64(:crypto.hash(:sha256, msg.data)))
  end

  def where_stream_id(query \\ base_query(), stream_id) do
    from([message: m] in query, where: m.stream_id == ^stream_id)
  end

  def where_subject_and_stream_id(query \\ base_query(), subject, stream_id) do
    from([message: m] in query, where: m.subject == ^subject and m.stream_id == ^stream_id)
  end

  def where_subject_and_stream_id_in(query \\ base_query(), subject_stream_id_pairs) do
    {subjects, stream_ids} = Enum.unzip(subject_stream_id_pairs)
    stream_ids = Enum.map(stream_ids, &UUID.string_to_binary!/1)

    from([message: m] in query,
      where:
        fragment(
          "(?, ?) IN (SELECT UNNEST(?::text[]), UNNEST(?::uuid[]))",
          m.subject,
          m.stream_id,
          ^subjects,
          ^stream_ids
        )
    )
  end

  @spec where_state(Ecto.Query.t(), atom()) :: Ecto.Query.t()
  def where_state(query \\ base_query(), state) do
    from([message: m] in query, where: m.state == ^state)
  end

  def where_seq_gt(query \\ base_query(), seq) do
    from([message: m] in query, where: m.seq > ^seq)
  end

  defp base_query(query \\ __MODULE__) do
    from(m in query, as: :message)
  end
end
