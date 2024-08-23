defmodule Sequin.Consumers.ConsumerEventData do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{
          record: map(),
          changes: map(),
          action: atom(),
          metadata: map()
        }

  @primary_key false
  @derive Jason.Encoder

  embedded_schema do
    field :record, :map
    field :changes, :map
    field :action, Ecto.Enum, values: [:insert, :update, :delete]

    embeds_one :metadata, Metadata, primary_key: false do
      @derive Jason.Encoder

      field :table_schema, :string
      field :table_name, :string
      field :commit_timestamp, :utc_datetime_usec
      field :consumer, :map
    end
  end

  def changeset(data, attrs) do
    data
    |> cast(attrs, [:record, :changes, :action])
    |> cast_embed(:metadata, required: true, with: &metadata_changeset/2)
    |> validate_required([:record, :action, :metadata])
  end

  def metadata_changeset(metadata, attrs) do
    metadata
    |> cast(attrs, [:table_schema, :table_name, :commit_timestamp])
    |> validate_required([:table_schema, :table_name, :commit_timestamp])
  end
end
