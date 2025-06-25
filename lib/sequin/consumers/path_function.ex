defmodule Sequin.Consumers.PathFunction do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias __MODULE__
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData

  @derive {Jason.Encoder, only: [:type, :path]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:path], default: :path
    field :path, :string
  end

  def changeset(struct, params, _account_id) do
    struct
    |> cast(params, [:path])
    |> validate_required([:path])
    |> validate_path()
  end

  defp validate_path(changeset) do
    changeset
    |> validate_format(
      :path,
      ~r/^[a-zA-Z0-9\-._~!$&'()*+,;=:@%\/]+$/,
      message: "must be a valid URL path"
    )
    |> validate_format(
      :path,
      ~r/^(record|changes|action|metadata)(\.[a-zA-Z0-9_\-.]+)*$/,
      message: "must be a valid path into the record structure"
    )
    |> validate_length(:path, max: 2000)
  end

  def apply(%PathFunction{path: path}, %ConsumerEventData{} = data) do
    traverse_path(data, String.split(path || "", "."))
  end

  def apply(%PathFunction{path: path}, %ConsumerRecordData{} = data) do
    traverse_path(data, String.split(path || "", "."))
  end

  # Carve out known structs that we can traverse
  defp traverse_path(%ConsumerEventData{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerRecordData{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerEventData.Metadata{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerRecordData.Metadata{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerEventData.Metadata.Sink{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerEventData.Metadata.Database{} = data, keys), do: mapify_struct_and_traverse(data, keys)
  defp traverse_path(%ConsumerRecordData.Metadata.Sink{} = data, keys), do: mapify_struct_and_traverse(data, keys)

  # Base case
  defp traverse_path(value, []), do: value

  # Traverse a map
  defp traverse_path(data, [key | rest]) when is_map(data) do
    case Map.get(data, key) do
      nil -> nil
      value when is_struct(value) -> traverse_path(value, rest)
      value when is_map(value) -> traverse_path(value, rest)
      value -> traverse_path(value, rest)
    end
  end

  # Traverse a list - we don't support this
  defp traverse_path(data, _keys) when is_list(data), do: nil

  # Traverse a struct
  defp mapify_struct_and_traverse(struct, keys) do
    struct
    |> Map.from_struct()
    |> Sequin.Map.stringify_keys()
    |> traverse_path(keys)
  end
end
