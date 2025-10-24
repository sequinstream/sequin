defmodule Sequin.String do
  @moduledoc false
  alias Sequin.Error

  @doc """
  Obfuscates a secret based on its length.

  - ≤5 chars: Full obfuscation
  - 6-9 chars: Preserve first and last
  - ≥10 chars: Keep first 3 and last char

  ## Examples

      iex> Sequin.String.obfuscate("12345")
      "*****"
      iex> Sequin.String.obfuscate("password")
      "p******d"
      iex> Sequin.String.obfuscate("secretcode")
      "sec******e"
  """
  def obfuscate(secret) when is_binary(secret) do
    case String.length(secret) do
      0 ->
        ""

      len when len <= 5 ->
        String.duplicate("*", len)

      len when len <= 9 ->
        first = String.first(secret)
        last = String.last(secret)
        middle = String.duplicate("*", len - 2)
        first <> middle <> last

      len ->
        first_three = String.slice(secret, 0, 3)
        last = String.last(secret)
        middle = String.duplicate("*", min(len - 4, 20))
        first_three <> middle <> last
    end
  end

  def uuid?(str) when is_binary(str) do
    str
    |> UUID.info()
    |> case do
      {:ok, info} -> info[:version] in [7, 4] and info[:variant] == :rfc4122
      _ -> false
    end
  end

  def binary_to_string(uuid) when is_binary(uuid) do
    Ecto.UUID.load(uuid)
  end

  def binary_to_string!(uuid) when is_binary(uuid) do
    case binary_to_string(uuid) do
      {:ok, uuid} ->
        uuid

      :error ->
        raise "Invalid UUID: #{inspect(uuid)}"
    end
  end

  def string_to_binary!(uuid) when is_binary(uuid) do
    UUID.string_to_binary!(uuid)
  end

  @doc """
  Checks if all strings in a list are valid UUIDs.

  ## Examples

      iex> Sequin.String.all_uuids?(["123e4567-e89b-12d3-a456-426614174000", "987fbc97-4bed-5078-9f07-9141ba07c9f3"])
      true

      iex> Sequin.String.all_uuids?(["123e4567-e89b-12d3-a456-426614174000", "not-a-uuid"])
      false

  """
  def all_uuids?(list) when is_list(list) do
    Enum.all?(list, &uuid?/1)
  end

  @doc """
  Converts a string into a valid  key identifier.

  ## Examples

      iex> Sequin.Key.to_key_token("My Country.My State>My Region")
      "my_country_my_state_my_region"

      iex> Sequin.Key.to_key_token("Hello, World! 123")
      "hello_world_123"

      iex> Sequin.Key.to_key_token("  Spaced  Out  ")
      "spaced_out"

      iex> Sequin.Key.to_key_token("Unsafe@#$%^&*Characters")
      "unsafe_characters"

  """
  def to_key_token(string) do
    string
    |> String.downcase()
    # Remove all non-word characters except spaces and hyphens
    |> String.replace(~r/[^\w\s-]/, "")
    # Replace spaces and hyphens with underscores
    |> String.replace(~r/[-\s]+/, "_")
    |> validate_token()
  end

  defp validate_token(""), do: "invalid_key_token"
  defp validate_token(identifier), do: identifier

  def downcase(nil), do: nil
  def downcase(string) when is_binary(string), do: String.downcase(string)

  def format_bytes(bytes) when bytes < 1024, do: "#{bytes} bytes"
  def format_bytes(bytes) when bytes < 1024 * 1024, do: "#{Float.round(bytes / 1024, 2)} KB"
  def format_bytes(bytes) when bytes < 1024 * 1024 * 1024, do: "#{Float.round(bytes / 1024 / 1024, 2)} MB"
  def format_bytes(bytes) when bytes < 1024 * 1024 * 1024 * 1024, do: "#{Float.round(bytes / 1024 / 1024 / 1024, 2)} GB"

  def to_existing_atom_safe(string) do
    {:ok, String.to_existing_atom(string)}
  rescue
    _ -> {:error, Error.invariant(message: "Not an existing atom: #{string}", code: :not_existing_atom)}
  end
end
