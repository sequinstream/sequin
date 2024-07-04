defmodule Sequin.String do
  @moduledoc false
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
        middle = String.duplicate("*", len - 4)
        first_three <> middle <> last
    end
  end

  @doc """
  Converts a string into a valid  subject identifier.

  ## Examples

      iex> Sequin.Subject.to_identifier("My Country.My State>My Region")
      "my_country_my_state_my_region"

      iex> Sequin.Subject.to_identifier("Hello, World! 123")
      "hello_world_123"

      iex> Sequin.Subject.to_identifier("  Spaced  Out  ")
      "spaced_out"

      iex> Sequin.Subject.to_identifier("Unsafe@#$%^&*Characters")
      "unsafe_characters"

  """
  def to_subject_token(string) do
    string
    |> String.downcase()
    # Remove all non-word characters except spaces and hyphens
    |> String.replace(~r/[^\w\s-]/, "")
    # Replace spaces and hyphens with underscores
    |> String.replace(~r/[-\s]+/, "_")
    |> validate_token()
  end

  defp validate_token(""), do: "invalid_subject_token"
  defp validate_token(identifier), do: identifier
end
