defmodule Sequin.HealthTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ErrorFactory
  alias Sequin.Health
  alias Sequin.Health.Check

  describe "initializes a new health" do
    test "initializes a new health" do
      entity = ConsumersFactory.consumer(id: Factory.uuid())
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :initializing
      assert is_list(health.checks) and length(health.checks) > 0
    end
  end

  describe "update/4" do
    test "updates the health of an entity with an expected check" do
      entity = ConsumersFactory.http_push_consumer(id: Factory.uuid())

      assert {:ok, %Health{} = health} = Health.update(entity, :receive, :healthy)
      assert health.status == :initializing
      assert Enum.find(health.checks, &(&1.id == :receive)).status == :healthy

      assert {:ok, %Health{} = health} = Health.update(entity, :push, :warning)
      assert health.status == :warning
      assert Enum.find(health.checks, &(&1.id == :push)).status == :warning
      assert Enum.find(health.checks, &(&1.id == :receive)).status == :healthy

      Enum.each(health.checks, fn check ->
        Health.update(entity, check.id, :healthy)
      end)

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :healthy
    end

    test "raises an error for unexpected checks" do
      entity = ConsumersFactory.http_push_consumer(id: Factory.uuid())

      assert_raise FunctionClauseError, fn ->
        Health.update(entity, :unexpected_check, :healthy)
      end
    end

    test "sets status to the worst status of any incoming check" do
      entity = ConsumersFactory.http_push_consumer(id: Factory.uuid())

      assert {:ok, %Health{} = health} = Health.update(entity, :receive, :healthy)
      assert health.status == :initializing

      assert {:ok, %Health{} = health} = Health.update(entity, :push, :warning)
      assert health.status == :warning

      assert {:ok, %Health{} = health} = Health.update(entity, :filters, :error, ErrorFactory.random_error())
      assert health.status == :error

      assert {:ok, %Health{} = health} = Health.update(entity, :receive, :healthy)
      # Still error because other checks are in error state
      assert health.status == :error
    end
  end

  describe "get/1" do
    test "finds the health of an entity" do
      entity = ConsumersFactory.consumer(id: Factory.uuid())

      assert {:ok, %Health{} = health} = Health.update(entity, :receive, :healthy)
      assert health.status == :initializing

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :initializing
    end
  end

  describe "to_external/1" do
    test "converts the health to an external format" do
      entity = ConsumersFactory.consumer(id: Factory.uuid())

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert external = Health.to_external(health)
      assert external.status == :initializing

      assert {:ok, %Health{} = health} = Health.update(entity, :receive, :error, ErrorFactory.random_error())
      assert external = Health.to_external(health)
      assert external.status == :error
      assert Enum.find(external.checks, &(not is_nil(&1.error)))
    end

    test ":postgres_database :replication_connected check is marked as erroring if it waiting for > 5 minutes" do
      entity = DatabasesFactory.postgres_database(id: Factory.uuid())
      assert {:ok, %Health{} = health} = Health.get(entity)

      ten_minutes_ago = DateTime.add(DateTime.utc_now(), -300, :second)

      health =
        update_in(health.checks, fn checks ->
          Enum.map(checks, fn
            %Check{id: :replication_connected} = check ->
              %{check | created_at: ten_minutes_ago}

            %Check{} = check ->
              check
          end)
        end)

      assert external = Health.to_external(health)
      assert external.status == :error
    end
  end
end
