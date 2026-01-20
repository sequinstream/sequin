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

    embeds_one :metadata, Metadata, primary_key: false, on_replace: :update do
      @derive Jason.Encoder

      field :table_schema, :string
      field :table_name, :string
      field :commit_timestamp, :utc_datetime_usec
      field :commit_lsn, :integer
      field :commit_idx, :integer
      field :database_name, :string
      field :transaction_annotations, :map
      field :idempotency_key, :string
      field :record_pks, {:array, :string}
      field :enrichment, :map

      embeds_one :consumer, Sink, primary_key: false, on_replace: :update do
        @derive Jason.Encoder

        field :id, :string
        field :name, :string
        field :annotations, :map
      end

      embeds_one :database, Database, primary_key: false, on_replace: :update do
        @derive Jason.Encoder

        field :id, :string
        field :name, :string
        field :annotations, :map
        field :database, :string
        field :hostname, :string
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
    |> cast(attrs, [
      :table_schema,
      :table_name,
      :commit_timestamp,
      :commit_lsn,
      :commit_idx,
      :database_name,
      :transaction_annotations,
      :idempotency_key,
      :record_pks,
      :enrichment
    ])
    |> validate_required([
      :table_schema,
      :table_name,
      :commit_timestamp,
      :commit_lsn,
      :commit_idx,
      :database_name,
      :idempotency_key
    ])
    |> cast_embed(:consumer, required: true, with: &consumer_changeset/2)
    |> cast_embed(:database, required: true, with: &database_changeset/2)
  end

  def consumer_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [:id, :name, :annotations])
    |> validate_required([:id, :name])
  end

  def database_changeset(database, attrs) do
    database
    |> cast(attrs, [:id, :name, :annotations, :database, :hostname])
    |> validate_required([:id, :name, :annotations, :database, :hostname])
  end

  def deserialize(%ConsumerEventData{} = data) do
    %{
      data
      | record: Sequin.Changeset.deserialize(data.record, data.record_deserializers),
        changes: Sequin.Changeset.deserialize(data.changes, data.changes_deserializers)
    }
  end

  def map_from_struct(%ConsumerEventData{} = data) do
    data
    |> Sequin.Map.from_ecto()
    |> update_in([:metadata], &Sequin.Map.from_ecto/1)
    |> update_in([:metadata, :consumer], &Sequin.Map.from_ecto/1)
    |> update_in([:metadata, :database], &Sequin.Map.from_ecto/1)
  end

  def struct_from_map(map) do
    map = Sequin.Map.atomize_keys(map)

    ConsumerEventData
    |> struct!(map)
    |> update_in([Access.key(:metadata)], fn metadata ->
      map = Sequin.Map.atomize_keys(metadata)
      struct!(ConsumerEventData.Metadata, map)
    end)
    |> update_in([Access.key(:metadata), Access.key(:consumer)], fn consumer ->
      map = Sequin.Map.atomize_keys(consumer)
      struct!(ConsumerEventData.Metadata.Sink, map)
    end)
    |> update_in([Access.key(:metadata), Access.key(:database)], fn
      database ->
        map = Sequin.Map.atomize_keys(database || %{})
        struct!(ConsumerEventData.Metadata.Database, map)
    end)
  end
end
