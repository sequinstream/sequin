defmodule Sequin.Consumers.SourceTable do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset
  import PolymorphicEmbed

  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Consumers.SourceTable.NullValue

  @type t :: %__MODULE__{
          oid: integer,
          schema_name: String.t(),
          table_name: String.t(),
          actions: [atom()],
          column_filters: [ColumnFilter.t()]
        }

  @type filter_type :: :string | :number | :boolean | :datetime

  defmodule ColumnFilter do
    @moduledoc false
    use Ecto.Schema

    import Ecto.Changeset

    @operators [:==, :!=, :>, :<, :>=, :<=, :in, :not_in, :is_null, :not_null]
    def operator_values, do: @operators

    def from_external_operator(nil), do: nil

    def from_external_operator(external_operator) do
      case String.downcase(external_operator) do
        "=" -> :==
        "!=" -> :!=
        ">" -> :>
        "<" -> :<
        ">=" -> :>=
        "<=" -> :<=
        "in" -> :in
        "not in" -> :not_in
        "is null" -> :is_null
        "not null" -> :not_null
        _ -> raise "Invalid operator: #{external_operator}"
      end
    end

    def from_external(%{
          "columnAttnum" => column_attnum,
          "operator" => operator,
          "valueType" => value_type,
          "value" => value
        }) do
      operator = from_external_operator(operator)

      value_type =
        case operator do
          :is_null -> :null
          :not_null -> :null
          :in -> :list
          :not_in -> :list
          _ -> value_type
        end

      value =
        case value_type do
          :list ->
            (value || "")
            |> String.trim()
            |> String.trim_leading("[")
            |> String.trim_leading("{")
            |> String.trim_trailing("]")
            |> String.trim_trailing("}")
            |> String.split(",")
            |> Enum.map(&String.trim/1)

          :null ->
            nil

          _ ->
            value
        end

      %{
        column_attnum: column_attnum,
        operator: operator,
        value: %{value: value, __type__: value_type}
      }
    end

    @type t :: %__MODULE__{
            column_attnum: integer,
            column_name: String.t(),
            operator: atom(),
            value: %{value: any()}
          }

    embedded_schema do
      field :column_attnum, :integer
      field :column_name, :string, virtual: true
      field :operator, Ecto.Enum, values: @operators

      polymorphic_embeds_one(:value,
        types: [
          string: Sequin.Consumers.SourceTable.StringValue,
          number: Sequin.Consumers.SourceTable.NumberValue,
          boolean: Sequin.Consumers.SourceTable.BooleanValue,
          datetime: Sequin.Consumers.SourceTable.DateTimeValue,
          list: Sequin.Consumers.SourceTable.ListValue,
          null: NullValue
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
      |> validate_null_value_operators()
    end

    defp validate_null_value_operators(changeset) do
      value = get_field(changeset, :value)
      operator = get_field(changeset, :operator)

      if is_struct(value, NullValue) and operator not in [:is_null, :not_null] do
        add_error(changeset, :operator, "must be either is_null or not_null for NullValue")
      else
        changeset
      end
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

defmodule Sequin.Consumers.SourceTable.NumberValue do
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
    case validate_boolean_format(params) do
      {:ok, validated_params} ->
        struct
        |> cast(validated_params, [:value])
        |> validate_required([:value])

      {:error, error} ->
        struct
        |> cast(%{}, [])
        |> add_error(:value, error)
    end
  end

  @error_msg ~s(must be either `true` or `false`)
  defp validate_boolean_format(%{value: value}) do
    validate_boolean_value(value)
  end

  defp validate_boolean_format(%{"value" => value}) do
    validate_boolean_value(value)
  end

  defp validate_boolean_format(_), do: {:error, @error_msg}

  defp validate_boolean_value(value) when is_boolean(value) do
    {:ok, %{value: value}}
  end

  defp validate_boolean_value(value) when is_binary(value) do
    case String.downcase(value) do
      "true" -> {:ok, %{value: true}}
      "false" -> {:ok, %{value: false}}
      _ -> {:error, @error_msg}
    end
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
    case validate_datetime_format(params) do
      :ok ->
        struct
        |> cast(params, [:value])
        |> validate_required([:value])

      {:error, error} ->
        struct
        |> cast(params, [])
        |> add_error(:value, error)
    end
  end

  defp validate_datetime_format(%{"value" => value}) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _, _} -> :ok
      {:error, _} -> {:error, "must be a valid UTC datetime in ISO 8601 format (e.g., 2023-04-13T14:30:00Z)"}
    end
  end

  defp validate_datetime_format(%{value: value}) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _, _} -> :ok
      {:error, _} -> {:error, "must be a valid UTC datetime in ISO 8601 format (e.g., 2023-04-13T14:30:00Z)"}
    end
  end

  defp validate_datetime_format(_), do: :ok
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

defmodule Sequin.Consumers.SourceTable.NullValue do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :value, :boolean
  end

  def changeset(struct, _params) do
    cast(struct, %{value: true}, [])
  end
end
