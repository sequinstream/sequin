defmodule Sequin.Runtime.SlotProducer.PostgresRelationHashCache do
  @moduledoc """
  Handles computing, and caching schema hashes for PostgreSQL relations.

  This module provides functionality to:
  - Compute a hash of a relation's schema using Erlang's phash2 function
  - Read, and update schema hashes using Redis

  Schema hashes are used to track changes in relation schemas over time.

  They are stored in a Redis hash using a key pattern defined in `schema_hash_key` below.
  """

  alias Sequin.Redis
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation

  require Logger

  @doc """
  Computes a hash of a relation's schema.
  """
  @spec compute_schema_hash(Relation.t()) :: String.t()
  def compute_schema_hash(%Relation{} = relation) do
    relation |> :erlang.phash2() |> to_string()
  end

  @doc """
  Gets a schema hash from Redis for a given relation.

  The hash is stored as a field in a hash with the relation ID as the field name.

  Returns the schema hash if found, nil otherwise.
  """
  @spec get_schema_hash(String.t(), integer()) :: String.t() | nil | {:error, any()}
  def get_schema_hash(database_id, relation_id) do
    case Redis.command(["HGET", schema_hash_key(database_id), relation_id]) do
      {:ok, nil} ->
        nil

      {:ok, hash} ->
        hash

      {:error, error} ->
        Logger.error("[SlotProcessorServer] Error getting schema hash",
          database_id: database_id,
          relation_id: relation_id,
          error: error
        )

        {:error, error}
    end
  end

  @doc """
  Updates the schema hash for a relation in Redis.

  The hash is stored as a field in a hash with the relation ID as the field name.

  Returns :ok on success, {:error, reason} on failure.
  """
  @spec update_schema_hash(String.t(), integer(), String.t()) :: :ok | {:error, any()}
  def update_schema_hash(database_id, relation_id, current_hash) do
    case Redis.command(["HSET", schema_hash_key(database_id), relation_id, current_hash]) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        Logger.error("[SlotProcessorServer] Error storing schema hash",
          database_id: database_id,
          relation_id: relation_id,
          error: error
        )

        {:error, error}
    end
  end

  @spec schema_hash_key(String.t()) :: String.t()
  defp schema_hash_key(database_id), do: "sequin:runtime:postgres_relation_hash_cache:#{database_id}"
end
