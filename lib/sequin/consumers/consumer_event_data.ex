defmodule Sequin.Consumers.ConsumerEventData do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  alias __MODULE__

  @type t :: %__MODULE__{
          record: map(),
          changes: map() | nil,
          action: atom(),
          metadata: map()
        }

  @primary_key false
  @derive {Jason.Encoder, except: [:record_deserializers, :changes_deserializers]}

  embedded_schema do
    field :record, :map
    field :record_deserializers, :map
    field :changes, :map
    field :changes_deserializers, :map
    field :action, Ecto.Enum, values: [:insert, :update, :delete, :read]

    embeds_one :metadata, Metadata, primary_key: false do
      @derive Jason.Encoder

      field :table_schema, :string
      field :table_name, :string
      field :commit_timestamp, :utc_datetime_usec
      field :commit_lsn, :integer
      field :database_name, :string
      field :transaction_annotations, :map
      field :idempotency_key, :string

      embeds_one :consumer, Sink, primary_key: false do
        @derive Jason.Encoder

        field :id, :string
        field :name, :string
        field :annotations, :map
      end
    end
  end

  def changeset(data, attrs) do
    data
    |> cast(attrs, [:record, :changes, :action])
    |> cast_embed(:metadata, required: true, with: &metadata_changeset/2)
    |> validate_required([:record, :action, :metadata])
    |> Sequin.Changeset.put_deserializers(:record, :record_deserializers)
    |> Sequin.Changeset.put_deserializers(:changes, :changes_deserializers)
  end

  def metadata_changeset(metadata, attrs) do
    metadata
    |> cast(attrs, [:table_schema, :table_name, :commit_timestamp, :commit_lsn, :database_name, :transaction_annotations])
    |> validate_required([:table_schema, :table_name, :commit_timestamp, :commit_lsn])
  end

  def deserialize(%ConsumerEventData{} = data) do
    %ConsumerEventData{
      data
      | record: Sequin.Changeset.deserialize(data.record, data.record_deserializers),
        changes: Sequin.Changeset.deserialize(data.changes, data.changes_deserializers)
    }
  end
end
