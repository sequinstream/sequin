defmodule Sequin.ConsumersRuntime.ConsumerProducerCacheTest do
  use Sequin.Case, async: true

  alias Sequin.ConsumersRuntime.ConsumerProducerCache
  alias Sequin.Factory

  def message(attrs \\ %{}) do
    attrs = Map.new(attrs)

    Map.merge(
      %{
        group_id: Factory.uuid(),
        ack_id: Factory.uuid(),
        not_visible_until: Factory.timestamp()
      },
      attrs
    )
  end

  describe "init/1" do
    test "properly groups messages by group_id for ack_ids_by_group_id" do
      group_id1 = Factory.uuid()
      group_id2 = Factory.uuid()

      messages = [
        message(group_id: group_id1),
        message(group_id: group_id1),
        message(group_id: group_id2)
      ]

      cache = ConsumerProducerCache.init(messages)

      assert map_size(cache.ack_ids_by_group_id) == 2
      assert MapSet.size(cache.ack_ids_by_group_id[group_id1]) == 2
      assert MapSet.size(cache.ack_ids_by_group_id[group_id2]) == 1
    end

    test "creates correct not_visible_until_unix_by_ack_id mapping" do
      now = DateTime.utc_now()
      one_hour_ago = DateTime.add(now, -3600)
      two_hours_ago = DateTime.add(now, -7200)

      messages = [
        message(ack_id: "ack1", not_visible_until: now),
        message(ack_id: "ack2", not_visible_until: one_hour_ago),
        message(ack_id: "ack3", not_visible_until: two_hours_ago)
      ]

      cache = ConsumerProducerCache.init(messages)

      assert map_size(cache.not_visible_until_unix_by_ack_id) == 3
      assert cache.not_visible_until_unix_by_ack_id["ack1"] == DateTime.to_unix(now, :millisecond)
      assert cache.not_visible_until_unix_by_ack_id["ack2"] == DateTime.to_unix(one_hour_ago, :millisecond)
      assert cache.not_visible_until_unix_by_ack_id["ack3"] == DateTime.to_unix(two_hours_ago, :millisecond)
    end
  end

  describe "remove_messages/2" do
    test "reduces group from 2 ack_ids to 1" do
      group_id = Factory.uuid()
      ack_id1 = Factory.uuid()
      ack_id2 = Factory.uuid()

      messages = [
        message(group_id: group_id, ack_id: ack_id1, not_visible_until: DateTime.utc_now()),
        message(group_id: group_id, ack_id: ack_id2, not_visible_until: DateTime.utc_now())
      ]

      cache = ConsumerProducerCache.init(messages)

      updated_cache =
        ConsumerProducerCache.remove_messages(cache, [
          %{group_id: group_id, ack_id: ack_id1}
        ])

      assert map_size(updated_cache.ack_ids_by_group_id) == 1
      assert MapSet.size(updated_cache.ack_ids_by_group_id[group_id]) == 1
      assert MapSet.member?(updated_cache.ack_ids_by_group_id[group_id], ack_id2)
    end

    test "removes group_id key when all ack_ids are removed" do
      group_id = Factory.uuid()
      ack_id1 = Factory.uuid()
      ack_id2 = Factory.uuid()

      messages = [
        message(group_id: group_id, ack_id: ack_id1, not_visible_until: DateTime.utc_now()),
        message(group_id: group_id, ack_id: ack_id2, not_visible_until: DateTime.utc_now())
      ]

      cache = ConsumerProducerCache.init(messages)

      updated_cache =
        ConsumerProducerCache.remove_messages(cache, [
          %{group_id: group_id, ack_id: ack_id1},
          %{group_id: group_id, ack_id: ack_id2}
        ])

      refute Map.has_key?(updated_cache.ack_ids_by_group_id, group_id)
    end

    test "drops elements from not_visible_until_unix_by_ack_id" do
      now = DateTime.utc_now()

      messages = [
        message(ack_id: "ack1", not_visible_until: now),
        message(ack_id: "ack2", not_visible_until: now)
      ]

      cache = ConsumerProducerCache.init(messages)

      updated_cache =
        ConsumerProducerCache.remove_messages(cache, [
          %{group_id: "group1", ack_id: "ack1"}
        ])

      assert map_size(updated_cache.not_visible_until_unix_by_ack_id) == 1
      assert Map.has_key?(updated_cache.not_visible_until_unix_by_ack_id, "ack2")
      refute Map.has_key?(updated_cache.not_visible_until_unix_by_ack_id, "ack1")
    end
  end

  describe "upsert_messages/2" do
    test "creates new mapset for non-existent group_id" do
      # Start with empty cache
      initial_messages = []
      cache = ConsumerProducerCache.init(initial_messages)

      group_id = Factory.uuid()
      ack_id = Factory.uuid()
      now = DateTime.utc_now()

      updated_cache =
        ConsumerProducerCache.upsert_messages(cache, [
          %{
            group_id: group_id,
            ack_id: ack_id,
            not_visible_until: now
          }
        ])

      assert Map.has_key?(updated_cache.ack_ids_by_group_id, group_id)
      assert MapSet.member?(updated_cache.ack_ids_by_group_id[group_id], ack_id)
    end

    test "adds ack_id to existing group_id's mapset" do
      group_id = Factory.uuid()
      ack_id1 = Factory.uuid()

      initial_messages = [
        message(group_id: group_id, ack_id: ack_id1, not_visible_until: DateTime.utc_now())
      ]

      cache = ConsumerProducerCache.init(initial_messages)

      ack_id2 = Factory.uuid()
      now = DateTime.utc_now()

      updated_cache =
        ConsumerProducerCache.upsert_messages(cache, [
          %{
            group_id: group_id,
            ack_id: ack_id2,
            not_visible_until: now
          }
        ])

      assert MapSet.member?(updated_cache.ack_ids_by_group_id[group_id], ack_id1)
      assert MapSet.member?(updated_cache.ack_ids_by_group_id[group_id], ack_id2)
    end

    test "updates not_visible_until_unix_by_ack_id with new messages" do
      cache = ConsumerProducerCache.init([])

      now = DateTime.utc_now()
      ack_id = Factory.uuid()

      updated_cache =
        ConsumerProducerCache.upsert_messages(cache, [
          %{
            group_id: Factory.uuid(),
            ack_id: ack_id,
            not_visible_until: now
          }
        ])

      assert updated_cache.not_visible_until_unix_by_ack_id[ack_id] == DateTime.to_unix(now, :millisecond)
    end
  end

  describe "group_id_persisted?/2" do
    test "returns false if cache is empty" do
      cache = ConsumerProducerCache.init([])

      refute ConsumerProducerCache.group_id_persisted?(cache, Factory.uuid())
    end

    test "returns true for existing group_id" do
      group_id = Factory.uuid()

      messages = [
        message(group_id: group_id, not_visible_until: DateTime.utc_now())
      ]

      cache = ConsumerProducerCache.init(messages)

      assert ConsumerProducerCache.group_id_persisted?(cache, group_id)
    end

    test "returns false for non-existent group_id" do
      group_id = Factory.uuid()

      messages = [
        message(group_id: group_id, not_visible_until: DateTime.utc_now())
      ]

      cache = ConsumerProducerCache.init(messages)

      refute ConsumerProducerCache.group_id_persisted?(cache, Factory.uuid())
    end

    test "returns false after group is removed" do
      group_id = Factory.uuid()
      ack_id = Factory.uuid()

      messages = [
        message(group_id: group_id, ack_id: ack_id, not_visible_until: DateTime.utc_now())
      ]

      cache = ConsumerProducerCache.init(messages)

      updated_cache =
        ConsumerProducerCache.remove_messages(cache, [
          %{group_id: group_id, ack_id: ack_id}
        ])

      refute ConsumerProducerCache.group_id_persisted?(updated_cache, group_id)
    end
  end

  describe "any_available_persisted_messages?/1" do
    test "returns false for empty cache" do
      cache = ConsumerProducerCache.init([])
      refute ConsumerProducerCache.any_available_persisted_messages?(cache)
    end

    test "returns true when any message has nil visibility" do
      now = DateTime.utc_now()

      messages = [
        message(ack_id: "ack1", not_visible_until: now),
        message(ack_id: "ack2", not_visible_until: nil)
      ]

      cache = ConsumerProducerCache.init(messages)
      assert ConsumerProducerCache.any_available_persisted_messages?(cache)
    end

    test "returns true when any message is visible" do
      now = DateTime.utc_now()
      one_hour_ago = DateTime.add(now, -3600)

      messages = [
        message(ack_id: "ack1", not_visible_until: one_hour_ago),
        message(ack_id: "ack2", not_visible_until: now)
      ]

      cache = ConsumerProducerCache.init(messages)
      assert ConsumerProducerCache.any_available_persisted_messages?(cache)
    end

    test "returns false when all messages are not yet visible" do
      now = DateTime.utc_now()
      one_hour_from_now = DateTime.add(now, 3600)
      two_hours_from_now = DateTime.add(now, 7200)

      messages = [
        message(ack_id: "ack1", not_visible_until: one_hour_from_now),
        message(ack_id: "ack2", not_visible_until: two_hours_from_now)
      ]

      cache = ConsumerProducerCache.init(messages)
      refute ConsumerProducerCache.any_available_persisted_messages?(cache)
    end

    test "updates availability after removing messages" do
      now = DateTime.utc_now()
      one_hour_from_now = DateTime.add(now, 3600)

      messages = [
        message(ack_id: "ack1", not_visible_until: now),
        message(ack_id: "ack2", not_visible_until: one_hour_from_now)
      ]

      cache = ConsumerProducerCache.init(messages)
      assert ConsumerProducerCache.any_available_persisted_messages?(cache)

      updated_cache =
        ConsumerProducerCache.remove_messages(cache, [
          %{group_id: "group1", ack_id: "ack1"}
        ])

      refute ConsumerProducerCache.any_available_persisted_messages?(updated_cache)
    end
  end
end
