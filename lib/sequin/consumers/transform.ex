defmodule Sequin.Consumers.Transform do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query
  import PolymorphicEmbed

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.FunctionTransform
  alias Sequin.Consumers.PathTransform
  alias Sequin.Consumers.RoutingTransform
  alias Sequin.Consumers.Transform

  @derive {Jason.Encoder, only: [:name, :type, :description, :transform]}
  typed_schema "transforms" do
    field :name, :string
    field :type, :string, read_after_writes: true
    field :description, :string

    belongs_to :account, Account

    polymorphic_embeds_one(:transform,
      types: [
        path: PathTransform,
        function: FunctionTransform,
        routing: RoutingTransform
      ],
      on_replace: :update,
      type_field_name: :type
    )

    timestamps()
  end

  def create_changeset(transform, attrs) do
    transform
    |> cast(attrs, [:name, :description])
    |> changeset(attrs)
    |> unique_constraint([:account_id, :name], error_key: :name)
  end

  def update_changeset(transform, attrs) do
    changeset(transform, attrs)
  end

  def changeset(transform, attrs) do
    account_id = if is_struct(transform, Transform), do: transform.account_id, else: transform.data.account_id

    transform
    |> cast(attrs, [:name, :description])
    |> cast_polymorphic_embed(:transform,
      required: true,
      with: [
        function: &FunctionTransform.changeset(&1, &2, account_id),
        routing: &RoutingTransform.changeset(&1, &2, account_id),
        path: &PathTransform.changeset(&1, &2, account_id)
      ]
    )
    |> validate_required([:name])
    |> validate_exclusion(:name, ["none", "null", "nil"])
    |> Sequin.Changeset.validate_name()
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
