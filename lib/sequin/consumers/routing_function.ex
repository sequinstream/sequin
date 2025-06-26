defmodule Sequin.Consumers.RoutingFunction do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Consumers

  @derive {Jason.Encoder, only: [:type, :code, :sink_type]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:routing], default: :routing

    field :sink_type, Ecto.Enum,
      values: [
        :http_push,
        :redis_string,
        :nats,
        :kafka
      ]

    field :code, :string
  end

  def changeset(struct, params, account_id) do
    changeset = cast(struct, params, [:code, :sink_type])

    if Sequin.feature_enabled?(account_id, :function_transforms) do
      changeset
      |> validate_required([:sink_type])
      |> validate_required([:code])
      |> validate_change(:code, fn :code, code ->
        Consumers.validate_code(code)
      end)
    else
      add_error(changeset, :type, "Function/routing transforms are not enabled. Talk to the Sequin team to enable them.")
    end
  end
end
