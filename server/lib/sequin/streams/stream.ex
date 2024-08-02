defmodule Sequin.Streams.Stream do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Streams
  alias Sequin.Streams.Stream

  @derive {Jason.Encoder, only: [:id, :name, :account_id, :one_message_per_key, :stats, :inserted_at, :updated_at]}
  typed_schema "streams" do
    field :name, :string
    field :one_message_per_key, :boolean
    field :stats, :map, virtual: true

    belongs_to :account, Account

    timestamps()
  end

  def changeset(%Stream{} = stream, attrs) do
    stream
    |> cast(attrs, [:name, :one_message_per_key])
    |> validate_required([:name])
    |> Sequin.Changeset.validate_name()
    |> unique_constraint([:account_id, :name], error_key: :name)
  end

  def where_id(query \\ base_query(), id) do
    from(s in query, where: s.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from(s in query, where: s.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.is_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_account_id(query \\ __MODULE__, account_id) do
    from(s in query, where: s.account_id == ^account_id)
  end

  def order_by(query \\ __MODULE__, order_by) do
    from(s in query, order_by: ^order_by)
  end

  defp base_query(query \\ __MODULE__) do
    from(s in query, as: :stream)
  end

  def load_stats(%Stream{id: stream_id} = stream) do
    %{
      stream
      | stats: %{
          message_count: Streams.fast_count_messages_for_stream(stream_id),
          consumer_count: Streams.count_consumers_for_stream(stream_id),
          storage_size: Streams.approximate_storage_size_for_stream(stream_id)
        }
    }
  end
end
