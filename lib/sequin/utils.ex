defmodule Sequin.Utils do
  @moduledoc false

  def wrap_ok(el), do: {:ok, el}

  def obfuscate(str) when is_binary(str) do
    str
    |> String.replace(~r/./, "*")
    |> String.slice(0..10)
  end

  def maybe_obfuscate(true, str), do: obfuscate(str)
  def maybe_obfuscate(false, str), do: str

  @doc """
  Traverse a changeset to extract errors into a map.

  This is useful for when you want to return a changeset error to the client.

  For example, if you have a changeset with an error message like this:

      "must be at least %{min} characters"

  And you want to return the error to the client as JSON, you can call this
  function to replace the %{min} placeholder with the actual value of the
  `min` key in the changeset's `errors` map.

  Additionally, this function will traverse the changeset's embeded changesets
  and merge their errors into the returned map.

  `key_mapper` allows you to transform the keys in the returned map. The mapper
  function will receive the struct and the key as arguments, and should return
  the transformed key.

  This allows us to transform error keys from the internal database column
  names to the external API field names.
  """
  def errors_on(%Ecto.Changeset{} = changeset) do
    errors = traverse_errors(changeset)

    embedded_errors =
      changeset.changes
      |> Enum.map(fn
        {key, %Ecto.Changeset{valid?: false} = embedded_changeset} ->
          {key, traverse_errors(embedded_changeset)}

        _ ->
          nil
      end)
      |> Enum.reject(&is_nil/1)
      |> Map.new()

    Map.merge(errors, embedded_errors)
  end

  defp traverse_errors(changeset) do
    changeset
    |> Ecto.Changeset.traverse_errors(fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _match, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
    |> Map.new()
  end
end
