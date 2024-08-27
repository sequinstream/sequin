defmodule Sequin.JSON do
  @moduledoc """
  Utilities for converting from JSON-style maps to Elixir structs.

  Assumes a JSON-style map is a map with string keys and JSON-encoded values.
  """

  alias Sequin.Time

  # Assumes that atoms are required fields
  @spec decode_atom(map(), String.t()) :: map()
  def decode_atom(json, key) do
    Map.update(json, key, nil, &convert_atom/1)
  end

  # Assumes that enums are required fields
  @spec decode_enum(map(), String.t(), list(atom())) :: map()
  def decode_enum(json, key, valid_atoms) do
    Map.update(json, key, nil, fn str ->
      atom = convert_atom(str)

      if Enum.member?(valid_atoms, atom) do
        atom
      else
        raise "Invalid enum value #{inspect(str)} for field #{inspect(key)}. Valid values are #{inspect(valid_atoms)}"
      end
    end)
  end

  defp convert_atom(nil), do: nil
  defp convert_atom(str), do: String.to_atom(str)

  @spec encode_polymorphic(map(), atom()) :: map()
  def encode_polymorphic(map, key) do
    Map.update!(map, key, &encode_struct_with_type/1)
  end

  @spec encode_struct_with_type(struct() | nil) :: map() | nil
  def encode_struct_with_type(nil), do: nil

  def encode_struct_with_type(%struct_name{} = struct) do
    %{
      _kind: struct_name,
      _struct: struct
    }
  end

  @spec decode_polymorphic(map(), String.t()) :: map()
  def decode_polymorphic(json, key) do
    Map.update(json, key, nil, &decode_struct_with_type/1)
  end

  @spec decode_struct_with_type(map() | nil) :: struct() | nil
  def decode_struct_with_type(nil), do: nil

  def decode_struct_with_type(%{"_kind" => module_name, "_struct" => original}) do
    module = String.to_atom(module_name)
    convert_struct(original, module)
  end

  # Assumes that nested structs are required fields
  @spec decode_struct(map(), String.t(), module()) :: map()
  def decode_struct(json, key, nested_module) do
    Map.update(json, key, nil, &convert_struct(&1, nested_module))
  end

  defp convert_struct(nil, _module), do: nil
  defp convert_struct(json, module), do: module.from_json(json)

  # Assumes that timestamps are optional fields
  @spec decode_timestamp(map(), String.t()) :: map()
  def decode_timestamp(json, key) do
    Map.update(json, key, nil, &parse_timestamp/1)
  end

  def decode_date(json, key) do
    Map.update(json, key, nil, &parse_date/1)
  end

  def migrate_key(json, old_key, new_key) do
    old_value = Map.get(json, old_key)
    Map.put_new(json, new_key, old_value)
  end

  defp parse_timestamp(nil), do: nil

  defp parse_timestamp(str) do
    Time.parse_timestamp!(str)
  end

  defp parse_date(nil), do: nil

  defp parse_date(str) do
    Time.parse_date!(str)
  end

  @spec struct(map(), module()) :: struct()
  def struct(json, module) do
    json
    |> Sequin.Map.atomize_keys()
    |> then(&Kernel.struct(module, &1))
  end

  def parse_with_schema(%{"type" => "object", "properties" => properties} = schema, payload, caster_module) do
    required = Map.get(schema, "required", [])

    Enum.reduce_while(properties, {:ok, %{}}, fn {prop_name, prop_schema}, {:ok, acc} ->
      case Map.fetch(payload, prop_name) do
        {:ok, value} ->
          case cast_property(prop_schema, value, caster_module) do
            {:ok, cast_value} ->
              {:cont, {:ok, Map.put(acc, prop_name, cast_value)}}

            {:error, _error} = error ->
              {:halt, error}
          end

        :error ->
          if prop_name in required do
            {:halt, {:error, %{prop_name => "Missing required property"}}}
          else
            {:cont, {:ok, acc}}
          end
      end
    end)
  end

  def parse_with_schema(_, _, _) do
    raise ArgumentError, "JSON schema is not valid"
  end

  defp cast_property(%{"type" => "object", "properties" => _} = schema, value, caster_module) do
    parse_with_schema(schema, value, caster_module)
  end

  defp cast_property(%{"type" => "array", "items" => items_schema}, values, caster_module) when is_list(values) do
    Enum.reduce(values, {:ok, []}, fn value, {:ok, acc} ->
      case cast_property(items_schema, value, caster_module) do
        {:ok, cast_value} -> {:ok, acc ++ [cast_value]}
        {:error, _} = error -> error
      end
    end)
  end

  defp cast_property(%{"type" => type} = schema, value, caster_module) do
    format = Map.get(schema, "format")
    caster_module.cast(value, type, format)
  end

  @spec decode_map_of_structs(map(), String.t(), module()) :: map()
  def decode_map_of_structs(json, key, module) do
    Map.update(json, key, %{}, fn map ->
      Map.new(map, fn {k, v} -> {k, convert_struct(v, module)} end)
    end)
  end
end
