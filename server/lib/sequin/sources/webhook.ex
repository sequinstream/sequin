defmodule Sequin.Sources.Webhook do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Ecto.Queryable

  @derive {Jason.Encoder, only: [:id, :name, :account_id, :stream_id, :stream, :inserted_at, :updated_at]}
  schema "webhooks" do
    field :name, :string

    belongs_to :account, Sequin.Accounts.Account
    belongs_to :stream, Sequin.Streams.Stream

    timestamps()
  end

  def create_changeset(webhook, attrs) do
    webhook
    |> cast(attrs, [:name, :stream_id])
    |> validate_required([:name, :stream_id])
    |> Sequin.Changeset.validate_name()
    |> foreign_key_constraint(:stream_id, name: "webhooks_stream_id_fkey")
    |> unique_constraint([:account_id, :name], error_key: :name)
  end

  def update_changeset(webhook, attrs) do
    webhook
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([webhook: w] in query, where: w.account_id == ^account_id)
  end

  @spec where_stream(Queryable.t(), String.t()) :: Queryable.t()
  def where_stream(query \\ base_query(), stream_id) do
    from([webhook: w] in query, where: w.stream_id == ^stream_id)
  end

  def where_id(query \\ base_query(), id) do
    from([webhook: w] in query, where: w.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from([webhook: w] in query, where: w.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.is_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  defp base_query(query \\ __MODULE__) do
    from(w in query, as: :webhook)
  end
end
