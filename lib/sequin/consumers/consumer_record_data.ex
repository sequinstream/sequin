defmodule Sequin.Consumers.ConsumerRecordData do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{
          record: map(),
          metadata: map()
        }

  @primary_key false
  @derive {Jason.Encoder, except: [:action]}
  embedded_schema do
    field :record, :map
    field :action, Ecto.Enum, values: [:insert, :update, :delete, :read]

    embeds_one :metadata, Metadata, primary_key: false do
      @derive Jason.Encoder

      field :table_schema, :string
      field :table_name, :string
      field :commit_timestamp, :utc_datetime_usec
      field :commit_lsn, :integer
      field :database_name, :string

      embeds_one :consumer, Sink, primary_key: false do
        @derive Jason.Encoder

        field :id, :string
        field :name, :string
      end
    end
  end

  def changeset(data, attrs) do
    data
    |> cast(attrs, [:record, :action])
    |> cast_embed(:metadata, required: true, with: &metadata_changeset/2)
    |> validate_required([:record, :metadata])
  end

  def metadata_changeset(metadata, attrs) do
    metadata
    |> cast(attrs, [:table_schema, :table_name, :commit_timestamp, :commit_lsn, :database_name])
    |> validate_required([:table_schema, :table_name, :commit_timestamp, :commit_lsn])
  end
end
