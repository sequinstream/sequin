defmodule Sequin.Runtime.Routing.Helpers do
  @moduledoc """
  Common utilities for routing info modules.
  Provides shared functionality without macro magic.
  """

  import Ecto.Changeset

  def validate_no_extra_keys(changeset, attrs, allowed_keys) do
    extra_keys = Enum.map(Map.keys(attrs), &to_string/1) -- Enum.map(allowed_keys, &to_string/1)

    case extra_keys do
      [] ->
        changeset

      _ ->
        Enum.reduce(extra_keys, changeset, fn key, acc ->
          add_error(acc, :unknown_field, key)
        end)
    end
  end

  @doc """
  Casts string-ish values to strings for fields that expect strings.
  Handles integers, atoms, floats, and booleans.

  ## Examples
  def validate_no_extra_keys(changeset, attrs, allowed_keys) do
    extra_keys = Enum.map(Map.keys(attrs), &to_string/1) -- Enum.map(allowed_keys, &to_string/1)

      iex> cast_string_fields(%{key: 123, action: :set})
      %{key: "123", action: "set"}

      iex> cast_string_fields(%{key: "already_string"})
      %{key: "already_string"}
  """
  def cast_numeric_to_string_fields(params, castable_keys) when is_map(params) do
    string_castable_keys = Enum.map(castable_keys, &to_string/1)

    Enum.reduce(params, %{}, fn {key, value}, acc ->
      string_key = to_string(key)

      casted_value =
        if (is_integer(value) or is_float(value)) and string_key in string_castable_keys do
          to_string(value)
        else
          value
        end

      Map.put(acc, key, casted_value)
    end)
  end

  @doc """
  Creates a struct from a module and attributes without validation.
  This allows for partial/incomplete structs that will be validated later.
  """
  def create_struct_without_validation(module, attrs) when is_map(attrs) do
    struct(module, attrs)
  end

  @doc """
  Merges a struct with a map of attributes and validates the result.
  Takes a struct and a map of attributes, and returns a struct with the attributes merged.
  """
  def merge_and_validate(struct, attrs) when is_struct(struct) and is_map(attrs) do
    module = struct.__struct__

    struct
    |> module.changeset(attrs)
    |> apply_action(:validate)
  end
end
