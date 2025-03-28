defmodule Sequin.Consumers.Transform do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query
  import PolymorphicEmbed

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.PathTransform
  alias Sequin.Databases.Sequence

  @derive {Jason.Encoder, only: [:id, :name, :type, :config, :account_id, :sequence_id]}
  schema "transforms" do
    field :name, :string
    field :type, :string, read_after_writes: true
    field :config, :map, default: %{}

    belongs_to :account, Account
    belongs_to :sequence, Sequence

    polymorphic_embeds_one(:transform,
      types: [
        path: PathTransform
      ],
      on_replace: :update,
      type_field_name: :type
    )

    timestamps()
  end

  def create_changeset(transform, attrs) do
    transform
    |> cast(attrs, [:name, :sequence_id])
    |> changeset(attrs)
    |> foreign_key_constraint(:sequence_id)
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> Sequin.Changeset.validate_name()
  end

  def update_changeset(transform, attrs) do
    changeset(transform, attrs)
  end

  def changeset(transform, attrs) do
    transform
    |> cast(attrs, [:name, :sequence_id])
    |> cast_polymorphic_embed(:transform, required: true)
    |> validate_required([:name, :sequence_id])
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([transform: t] in query, where: t.account_id == ^account_id)
  end

  def where_sequence_id(query \\ base_query(), sequence_id) do
    from([transform: t] in query, where: t.sequence_id == ^sequence_id)
  end

  def where_id(query \\ base_query(), id) do
    from([transform: t] in query, where: t.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from([transform: t] in query, where: t.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.is_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  defp base_query(query \\ __MODULE__) do
    from(t in query, as: :transform)
  end
end
