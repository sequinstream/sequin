defmodule Sequin.Consumers.SourceTable do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset
  import PolymorphicEmbed

  defmodule ColumnFilter do
    @moduledoc false
    use Ecto.Schema

    import Ecto.Changeset

    @operators [:==, :!=, :>, :<, :>=, :<=, :in, :not_in, :is_null, :not_null]
    def operator_values, do: @operators

    embedded_schema do
      field :column_attnum, :integer
      field :column_name, :string, virtual: true
      field :operator, Ecto.Enum, values: @operators

      polymorphic_embeds_one(:value,
        types: [
          string: Sequin.Consumers.SourceTable.StringValue,
          integer: Sequin.Consumers.SourceTable.IntegerValue,
          float: Sequin.Consumers.SourceTable.FloatValue,
          boolean: Sequin.Consumers.SourceTable.BooleanValue,
          datetime: Sequin.Consumers.SourceTable.DateTimeValue,
          list: Sequin.Consumers.SourceTable.ListValue
        ],
        on_replace: :update
      )
    end

    def changeset(column_filter, attrs) do
      column_filter
      |> cast(attrs, [:column_attnum, :column_name, :operator])
      |> cast_polymorphic_embed(:value)
      |> validate_required([:column_attnum, :operator, :value])
      |> validate_inclusion(:operator, @operators)
    end
  end

  @primary_key false
  embedded_schema do
    field :oid, :integer
    field :schema_name, :string, virtual: true
    field :table_name, :string, virtual: true
    field :actions, {:array, Ecto.Enum}, values: [:insert, :update, :delete]
    embeds_many :column_filters, ColumnFilter
  end

  def changeset(source_table, attrs) do
    source_table
    |> cast(attrs, [:oid, :schema_name, :table_name, :actions])
    |> validate_required([:oid, :actions])
    |> cast_embed(:column_filters, with: &ColumnFilter.changeset/2)
    |> validate_length(:actions, min: 1)
  end
end

defmodule Sequin.Consumers.SourceTable.StringValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:value])
    |> validate_required([:value])
  end
end

defmodule Sequin.Consumers.SourceTable.IntegerValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, :integer
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:value])
    |> validate_required([:value])
  end
end

defmodule Sequin.Consumers.SourceTable.FloatValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, :float
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:value])
    |> validate_required([:value])
  end
end

defmodule Sequin.Consumers.SourceTable.BooleanValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, :boolean
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:value])
    |> validate_required([:value])
  end
end

defmodule Sequin.Consumers.SourceTable.DateTimeValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, :utc_datetime
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:value])
    |> validate_required([:value])
  end
end

defmodule Sequin.Consumers.SourceTable.ListValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, {:array, :any}
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:value])
    |> validate_required([:value])
  end
end
