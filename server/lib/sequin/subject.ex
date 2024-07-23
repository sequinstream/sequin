defmodule Sequin.Key do
  @moduledoc """
  This module provides functionality for working with keys and key patterns.

  A key is a `.` delimited string used for message routing in a publish-subscribe system.
  A key pattern is used for subscribing to one or more keys using wildcards.

  Valid keys:
  - Must not be empty
  - Must not contain spaces, `*`, or `>`
  - Must not start or end with a delimiter (`.`)
  - Must not contain empty tokens (`..`)
  - Must not exceed 255 characters
  - Must not contain more than 16 tokens

  Valid key patterns:
  - Follow the same rules as keys, with the addition of wildcards
  - Can use `*` to match a single token
  - Can use `>` to match one or more tokens, but only at the end of the pattern

  Examples:
  - Valid key: "orders.new.store123"
  - Valid key pattern: "orders.*.store123"
  - Valid key pattern: "orders.>"

  A key pattern matches a key as in the examples below:

  - "orders.*" would match "orders.new" and "orders.update", but not "orders.new.priority"
  - "orders.>" would match "orders.new", "orders.update", and "orders.new.priority"

  Without the `>` character, the key_pattern must exactly match in length to a message key.

  It's recommended to use alphanumeric characters, `-` (dash), and `_` (underscore) for key
  names to ensure compatibility across different systems and configurations.
  """

  @doc """
  Validates a key string.

  Returns :ok if the key is valid, or {:error, reason} if it's invalid.
  """
  def validate_key(key) when is_binary(key) do
    cond do
      key == "" ->
        {:error, "Key must not be empty"}

      String.contains?(key, [" ", "*", ">"]) ->
        {:error, "Key contains invalid characters"}

      String.starts_with?(key, ".") or String.ends_with?(key, ".") ->
        {:error, "Key must not start or end with a delimiter"}

      String.contains?(key, "..") ->
        {:error, "Key must not contain empty tokens"}

      String.length(key) > 255 ->
        {:error, "Key must not exceed 255 characters"}

      key |> String.split(".") |> length() > 16 ->
        {:error, "Key must not contain more than 16 tokens"}

      true ->
        :ok
    end
  end

  @doc """
  Validates a key pattern string.

  Returns :ok if the pattern is valid, or {:error, reason} if it's invalid.
  """
  def validate_key_pattern(pattern) when is_binary(pattern) do
    tokens = String.split(pattern, ".")

    cond do
      pattern == "" ->
        {:error, "Key pattern must not be empty"}

      String.starts_with?(pattern, ".") or String.ends_with?(pattern, ".") ->
        {:error, "Key pattern must not start or end with a delimiter"}

      String.contains?(pattern, "..") ->
        {:error, "Key pattern must not contain empty tokens"}

      String.length(pattern) > 255 ->
        {:error, "Key pattern must not exceed 255 characters"}

      length(tokens) > 16 ->
        {:error, "Key pattern must not contain more than 16 tokens"}

      ">" in Enum.slice(tokens, 0..-2//1) ->
        {:error, "Wildcard '>' can only appear at the end of the pattern"}

      true ->
        :ok
    end
  end

  @doc """
  Checks if a key matches a given pattern.

  Returns true if the key matches the pattern, false otherwise.
  """
  def matches?(pattern, key) do
    pattern_tokens = String.split(pattern, ".")
    key_tokens = String.split(key, ".")

    cond do
      List.last(pattern_tokens) == ">" ->
        match_with_trailing_wildcard(Enum.drop(pattern_tokens, -1), key_tokens)

      length(pattern_tokens) != length(key_tokens) ->
        false

      true ->
        match_tokens(pattern_tokens, key_tokens)
    end
  end

  defp match_with_trailing_wildcard(pattern_tokens, key_tokens) do
    pattern_length = length(pattern_tokens)
    key_length = length(key_tokens)

    key_length > pattern_length and
      match_tokens(pattern_tokens, Enum.take(key_tokens, pattern_length))
  end

  defp match_tokens(pattern_tokens, key_tokens) do
    pattern_tokens
    |> Enum.zip(key_tokens)
    |> Enum.all?(fn {pattern_token, key_token} ->
      pattern_token == "*" or pattern_token == key_token
    end)
  end

  @doc """
  Converts a string into a valid key token.

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

  @doc """
  Tokenizes a key pattern into a list of tokens.

  ## Examples

      iex> Sequin.Key.tokenize_pattern("orders.*.store123")
      ["orders", "*", "store123"]

      iex> Sequin.Key.tokenize_pattern("orders.>")
      ["orders", ">"]

  """
  def tokenize_pattern(pattern) do
    String.split(pattern, ".")
  end
end
