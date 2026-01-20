defmodule Sequin.Consumers.Function do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query
  import PolymorphicEmbed

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.EnrichmentFunction
  alias Sequin.Consumers.FilterFunction
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.PathFunction
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Consumers.TransformFunction

  @derive {Jason.Encoder, only: [:name, :type, :description, :function]}
  typed_schema "functions" do
    field :name, :string
    field :type, :string, read_after_writes: true
    field :description, :string

    belongs_to :account, Account

    polymorphic_embeds_one(:function,
      types: [
        path: PathFunction,
        transform: TransformFunction,
        routing: RoutingFunction,
        filter: FilterFunction,
        enrichment: EnrichmentFunction
      ],
      on_replace: :update,
      type_field_name: :type
    )

    timestamps()
  end

  def create_changeset(function, attrs) do
    function
    |> cast(attrs, [:name, :description])
    |> changeset(attrs)
    |> unique_constraint([:account_id, :name], error_key: :name)
  end

  def update_changeset(function, attrs) do
    changeset(function, attrs)
  end

  def changeset(function, attrs) do
    account_id = if is_struct(function, Function), do: function.account_id, else: function.data.account_id

    function
    |> cast(attrs, [:name, :description])
    |> cast_polymorphic_embed(:function,
      required: true,
      with: [
        transform: &TransformFunction.changeset(&1, &2, account_id),
        routing: &RoutingFunction.changeset(&1, &2, account_id),
        filter: &FilterFunction.changeset(&1, &2, account_id),
        path: &PathFunction.changeset(&1, &2, account_id),
        enrichment: &EnrichmentFunction.changeset(&1, &2, account_id)
      ]
    )
    |> validate_required([:name])
    |> validate_exclusion(:name, ["none", "null", "nil"])
    |> Sequin.Changeset.validate_name()
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([function: t] in query, where: t.account_id == ^account_id)
  end

  def where_sequence_id(query \\ base_query(), sequence_id) do
    from([function: t] in query, where: t.sequence_id == ^sequence_id)
  end

  def where_id(query \\ base_query(), id) do
    from([function: t] in query, where: t.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from([function: t] in query, where: t.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.sequin_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  defp base_query(query \\ __MODULE__) do
    from(t in query, as: :function)
  end
end
