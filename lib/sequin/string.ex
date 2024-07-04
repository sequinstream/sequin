defmodule Sequin.String do
  @moduledoc false
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
