defmodule Sequin.Subject do
  @moduledoc """
  This module provides functionality for working with subjects and subject patterns.

  A subject is a `.` delimited string used for message routing in a publish-subscribe system.
  A subject pattern is used for subscribing to one or more subjects using wildcards.

  Valid subjects:
  - Must not be empty
  - Must not contain spaces, `*`, or `>`
  - Must not start or end with a delimiter (`.`)
  - Must not contain empty tokens (`..`)
  - Must not exceed 255 characters
  - Must not contain more than 16 tokens

  Valid subject patterns:
  - Follow the same rules as subjects, with the addition of wildcards
  - Can use `*` to match a single token
  - Can use `>` to match one or more tokens, but only at the end of the pattern

  Examples:
  - Valid subject: "orders.new.store123"
  - Valid subject pattern: "orders.*.store123"
  - Valid subject pattern: "orders.>"

  A subject pattern matches a subject as in the examples below:

  - "orders.*" would match "orders.new" and "orders.update", but not "orders.new.priority"
  - "orders.>" would match "orders.new", "orders.update", and "orders.new.priority"

  Without the `>` character, the subject_pattern must exactly match in length to a message subject.

  It's recommended to use alphanumeric characters, `-` (dash), and `_` (underscore) for subject
  names to ensure compatibility across different systems and configurations.
  """

  @doc """
  Validates a subject string.

  Returns :ok if the subject is valid, or {:error, reason} if it's invalid.
  """
  def validate_subject(subject) when is_binary(subject) do
    cond do
      subject == "" ->
        {:error, "Subject must not be empty"}

      String.contains?(subject, [" ", "*", ">"]) ->
        {:error, "Subject contains invalid characters"}

      String.starts_with?(subject, ".") or String.ends_with?(subject, ".") ->
        {:error, "Subject must not start or end with a delimiter"}

      String.contains?(subject, "..") ->
        {:error, "Subject must not contain empty tokens"}

      String.length(subject) > 255 ->
        {:error, "Subject must not exceed 255 characters"}

      subject |> String.split(".") |> length() > 16 ->
        {:error, "Subject must not contain more than 16 tokens"}

      true ->
        :ok
    end
  end

  @doc """
  Validates a subject pattern string.

  Returns :ok if the pattern is valid, or {:error, reason} if it's invalid.
  """
  def validate_subject_pattern(pattern) when is_binary(pattern) do
    tokens = String.split(pattern, ".")

    cond do
      pattern == "" ->
        {:error, "Subject pattern must not be empty"}

      String.starts_with?(pattern, ".") or String.ends_with?(pattern, ".") ->
        {:error, "Subject pattern must not start or end with a delimiter"}

      String.contains?(pattern, "..") ->
        {:error, "Subject pattern must not contain empty tokens"}

      String.length(pattern) > 255 ->
        {:error, "Subject pattern must not exceed 255 characters"}

      length(tokens) > 16 ->
        {:error, "Subject pattern must not contain more than 16 tokens"}

      ">" in Enum.slice(tokens, 0..-2//1) ->
        {:error, "Wildcard '>' can only appear at the end of the pattern"}

      true ->
        :ok
    end
  end

  @doc """
  Checks if a subject matches a given pattern.

  Returns true if the subject matches the pattern, false otherwise.
  """
  def matches?(pattern, subject) do
    pattern_tokens = String.split(pattern, ".")
    subject_tokens = String.split(subject, ".")

    cond do
      List.last(pattern_tokens) == ">" ->
        match_with_trailing_wildcard(Enum.drop(pattern_tokens, -1), subject_tokens)

      length(pattern_tokens) != length(subject_tokens) ->
        false

      true ->
        match_tokens(pattern_tokens, subject_tokens)
    end
  end

  defp match_with_trailing_wildcard(pattern_tokens, subject_tokens) do
    pattern_length = length(pattern_tokens)
    subject_length = length(subject_tokens)

    subject_length > pattern_length and
      match_tokens(pattern_tokens, Enum.take(subject_tokens, pattern_length))
  end

  defp match_tokens(pattern_tokens, subject_tokens) do
    pattern_tokens
    |> Enum.zip(subject_tokens)
    |> Enum.all?(fn {pattern_token, subject_token} ->
      pattern_token == "*" or pattern_token == subject_token
    end)
  end

  @doc """
  Converts a string into a valid subject token.

  ## Examples

      iex> Sequin.Subject.to_subject_token("My Country.My State>My Region")
      "my_country_my_state_my_region"

      iex> Sequin.Subject.to_subject_token("Hello, World! 123")
      "hello_world_123"

      iex> Sequin.Subject.to_subject_token("  Spaced  Out  ")
      "spaced_out"

      iex> Sequin.Subject.to_subject_token("Unsafe@#$%^&*Characters")
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
