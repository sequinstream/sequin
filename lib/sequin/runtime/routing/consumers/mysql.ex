defmodule Sequin.Runtime.Routing.Consumers.Mysql do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  alias Sequin.Runtime.Routing

  @primary_key false
  @derive {Jason.Encoder, only: [:table_name]}
  typed_embedded_schema do
    field :table_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:table_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:table_name])
    |> validate_length(:table_name, min: 1, max: 64)
    |> validate_format(:table_name, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/,
      message: "must be a valid MySQL table name (alphanumeric and underscores, starting with letter or underscore)"
    )
  end

  def route(_action, _record, _changes, metadata) do
    # Default routing: use the source table name, but replace schema separator
    table_name =
      if metadata.table_schema do
        "#{metadata.table_schema}_#{metadata.table_name}"
      else
        metadata.table_name
      end

    %{table_name: sanitize_table_name(table_name)}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{table_name: sink.table_name}
  end

  # Private helper to sanitize table names for MySQL
  defp sanitize_table_name(name) do
    name
    |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
    # Ensure it doesn't start with a number
    |> String.replace(~r/^[0-9]/, "_\\0")
    # MySQL table name limit
    |> String.slice(0, 64)
  end
end
