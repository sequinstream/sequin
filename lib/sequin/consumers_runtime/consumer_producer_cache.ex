defmodule Sequin.ConsumersRuntime.ConsumerProducerCache do
  @moduledoc """
  Maintains in-memory state of message metadata for messages persisted in Postgres.

  This cache serves two critical purposes:

  1. Group ID Tracking:
     - Tracks which group IDs have messages saved in Postgres
     - Used to determine if new messages from the slot message store should be blocked
     - If a group ID exists in Postgres, new messages for that group must also be persisted

  2. Visibility Timing:
     - Caches the minimum "not visible until" timestamp across all messages
     - Enables efficient scheduling of when to next check Postgres for deliverable messages
     - Prevents unnecessary database polling

  The cache maintains three key pieces of state:
  - ack_ids_by_group_id: Maps group IDs to sets of acknowledgment IDs
  - not_visible_until_unix_by_ack_id: Maps ack IDs to their visibility timestamps
  """
  use TypedStruct

  @type group_id :: String.t()
  @type ack_id :: String.t()
  @type unix_timestamp :: integer()

  @type message_metadata :: %{
          group_id: group_id(),
          ack_id: ack_id(),
          not_visible_until_unix: unix_timestamp() | nil
        }

  typedstruct do
    field :ack_ids_by_group_id, %{group_id() => MapSet.t(ack_id())}, enforce: true
    field :not_visible_until_unix_by_ack_id, %{ack_id() => unix_timestamp() | nil}, enforce: true
  end

  @doc """
  Initializes a new cache with the given messages.

  Takes a list of messages and creates the initial cache state by:
  - Grouping ack_ids by their group_id
  - Creating a mapping of visibility timestamps
  - Computing the minimum visibility timestamp

  ## Parameters
    - messages: List of message structs containing group_id, ack_id, and not_visible_until
  """
  @spec init([message_metadata()]) :: t()
  def init(messages) do
    ack_ids_by_group_id =
      messages
      |> Enum.group_by(& &1.group_id, & &1.ack_id)
      |> Map.new(fn {group_id, ack_ids} -> {group_id, MapSet.new(ack_ids)} end)

    not_visible_until_by_ack_id =
      Enum.reduce(messages, %{}, fn message, acc ->
        unix_ts =
          if message.not_visible_until do
            DateTime.to_unix(message.not_visible_until, :millisecond)
          end

        Map.put(acc, message.ack_id, unix_ts)
      end)

    %__MODULE__{
      ack_ids_by_group_id: ack_ids_by_group_id,
      not_visible_until_unix_by_ack_id: not_visible_until_by_ack_id
    }
  end

  @doc """
  Removes messages from the cache.

  Updates cache state when messages are acknowledged or expired by:
  - Removing ack_ids from their group's set
  - Removing empty groups
  - Updating visibility timestamp mappings
  - Recalculating minimum visibility if needed

  ## Parameters
    - cache: Current cache state
    - message_metadata_by_ack_id: Map of ack_ids to their metadata
  """
  @spec remove_messages(t(), [message_metadata()]) :: t()
  def remove_messages(cache, messages) do
    ack_ids = Enum.map(messages, & &1.ack_id)

    next_ack_ids_by_group_id =
      Enum.reduce(messages, cache.ack_ids_by_group_id, fn %{ack_id: ack_id, group_id: group_id}, acc ->
        next_ack_ids = acc |> Map.get(group_id, MapSet.new([])) |> MapSet.delete(ack_id)

        if MapSet.equal?(next_ack_ids, MapSet.new([])) do
          Map.delete(acc, group_id)
        else
          Map.put(acc, group_id, next_ack_ids)
        end
      end)

    {_dropped, next_not_visible_until_unix_by_ack_id} =
      Map.split(cache.not_visible_until_unix_by_ack_id, ack_ids)

    %__MODULE__{
      ack_ids_by_group_id: next_ack_ids_by_group_id,
      not_visible_until_unix_by_ack_id: next_not_visible_until_unix_by_ack_id
    }
  end

  @doc """
  Adds or updates messages in the cache.

  Updates cache state with new message information by:
  - Adding ack_ids to their group's set
  - Creating new groups as needed
  - Updating visibility timestamp mappings
  - Recalculating minimum visibility

  ## Parameters
    - cache: Current cache state
    - message_metadata_by_ack_id: Map of ack_ids to their metadata
  """
  @spec upsert_messages(t(), [message_metadata()]) :: t()
  def upsert_messages(cache, messages) do
    next_ack_ids_by_group_id =
      Enum.reduce(messages, cache.ack_ids_by_group_id, fn %{ack_id: ack_id, group_id: group_id}, acc ->
        Map.update(acc, group_id, MapSet.new([ack_id]), fn ack_ids -> MapSet.put(ack_ids, ack_id) end)
      end)

    next_not_visible_until_unix_by_ack_id =
      Enum.reduce(messages, cache.not_visible_until_unix_by_ack_id, fn %{
                                                                         ack_id: ack_id,
                                                                         not_visible_until: not_visible_until
                                                                       },
                                                                       acc ->
        Map.put(acc, ack_id, DateTime.to_unix(not_visible_until, :millisecond))
      end)

    %__MODULE__{
      ack_ids_by_group_id: next_ack_ids_by_group_id,
      not_visible_until_unix_by_ack_id: next_not_visible_until_unix_by_ack_id
    }
  end

  @doc """
  Checks if a group_id has any messages persisted in Postgres.

  ## Parameters
    - cache: Current cache state
    - group_id: The group ID to check

  ## Returns
    - true if the group_id exists in the cache
    - false if the group_id is not found
  """
  def group_id_persisted?(cache, group_id) do
    Map.has_key?(cache.ack_ids_by_group_id, group_id)
  end

  @doc """
  Checks if there are any available persisted messages.
  """
  def any_available_persisted_messages?(cache) do
    case next_min_not_visible_until(cache) do
      :no_messages -> false
      nil -> true
      next_not_visible_until -> DateTime.before?(next_not_visible_until, DateTime.utc_now())
    end
  end

  @spec next_min_not_visible_until(cache :: t()) :: DateTime.t() | :no_messages
  defp next_min_not_visible_until(cache) do
    values = Map.values(cache.not_visible_until_unix_by_ack_id)

    if values == [] do
      :no_messages
    else
      min =
        Enum.min(
          values,
          # default sort is <=/2, but that `nil <= 100` is false. We want it to be true
          # so that nil is the lowest value
          fn
            nil, _b -> true
            _a, nil -> false
            a, b -> a <= b
          end
        )

      if min do
        DateTime.from_unix!(min, :millisecond)
      end
    end
  end
end
