defmodule Sequin.Streams.Message do
  @moduledoc false
  use Sequin.StreamSchema

  import Ecto.Changeset
  import Ecto.Query

  @derive {Jason.Encoder, only: [:subject, :stream_id, :data_hash, :data, :seq, :inserted_at, :updated_at]}

  @primary_key false
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

    case Sequin.Subject.validate_subject(subject) do
      :ok -> changeset
      {:error, reason} -> add_error(changeset, :subject, reason)
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

  def where_subject_in(query \\ base_query(), subjects) do
    from([message: m] in query, where: m.subject in ^subjects)
  end

  def where_subject_pattern(query \\ base_query(), pattern) do
    tokens = Sequin.Subject.tokenize_pattern(pattern)
    trailing_wildcard = List.last(tokens) == ">"

    query =
      tokens
      |> Enum.with_index(1)
      |> Enum.reduce(query, fn
        {"*", _index}, acc ->
          acc

        {">", index}, acc ->
          field_name = :"token#{index}"
          where(acc, [message: m], not is_nil(field(m, ^field_name)))

        {token, index}, acc ->
          field_name = :"token#{index}"
          where(acc, [message: m], field(m, ^field_name) == ^token)
      end)

    if trailing_wildcard do
      query
    else
      # If there's no trailing wildcard, we want to make sure the n+1'th token is nil
      field_name = :"token#{length(tokens) + 1}"
      where(query, [message: m], is_nil(field(m, ^field_name)))
    end
  end

  defp base_query(query \\ __MODULE__) do
    from(m in query, as: :message)
  end
end
