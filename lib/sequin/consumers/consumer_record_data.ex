defmodule Sequin.Consumers.ConsumerRecordData do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  alias __MODULE__

  @type t :: %__MODULE__{
          record: map(),
          metadata: map()
        }

  @primary_key false
  @derive {Jason.Encoder, except: [:action, :record_deserializers]}
  embedded_schema do
    field :record, :map
    field :record_deserializers, :map
    field :action, Ecto.Enum, values: [:insert, :update, :delete, :read]

    embeds_one :metadata, Metadata, primary_key: false, on_replace: :update do
      @derive Jason.Encoder

      field :table_schema, :string
      field :table_name, :string
      field :commit_timestamp, :utc_datetime_usec
      field :commit_lsn, :integer
      field :database_name, :string
      field :idempotency_key, :string
      field :record_pks, {:array, :string}

      embeds_one :consumer, Sink, primary_key: false, on_replace: :update do
        @derive Jason.Encoder

        field :id, :string
        field :name, :string
        field :annotations, :map
      end
    end
  end

  def changeset(data, attrs) do
    data
    |> cast(attrs, [:record, :action])
    |> cast_embed(:metadata, required: true, with: &metadata_changeset/2)
    |> validate_required([:record, :metadata])
    |> Sequin.Changeset.put_deserializers(:record, :record_deserializers)
  end

  def metadata_changeset(metadata, attrs) do
    metadata
    |> cast(attrs, [
      :table_schema,
      :table_name,
      :commit_timestamp,
      :commit_lsn,
      :database_name,
      :idempotency_key,
      :record_pks
    ])
    |> validate_required([:table_schema, :table_name, :commit_timestamp, :commit_lsn, :record_pks])
    |> cast_embed(:consumer, required: true, with: &consumer_changeset/2)
  end

  def consumer_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [:id, :name, :annotations])
    |> validate_required([:id, :name])
  end

  def deserialize(%ConsumerRecordData{} = data) do
    %{data | record: Sequin.Changeset.deserialize(data.record, data.record_deserializers)}
  end
end
