defmodule Sequin.Consumers.ConsumerEventData do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  alias Sequin.Consumers.ConsumerMessageMetadata

  @primary_key false
  @derive Jason.Encoder

  embedded_schema do
    field :record, :map
    field :changes, :map
    field :action, Ecto.Enum, values: [:insert, :update, :delete]

    embeds_one :metadata, ConsumerMessageMetadata
  end

  def changeset(data, attrs) do
    data
    |> cast(attrs, [:record, :changes, :action])
    |> cast_embed(:metadata, required: true)
    |> validate_required([:record, :action, :metadata])
  end
end
