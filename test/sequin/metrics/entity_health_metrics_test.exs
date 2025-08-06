defmodule Sequin.Metrics.EntityHealthMetricsTest do
  use Sequin.DataCase

  alias Sequin.Factory.HealthFactory
  alias Sequin.Metrics.EntityHealthMetrics
  alias Sequin.PrometheusMock

  describe "report_entity_health/1" do
    test "resets gauge and updates metrics based on health snapshots" do
      # Set up expectations for PrometheusMock
      expect(PrometheusMock, :reset_entity_health_gauge, fn -> :ok end)

      healthy =
        HealthFactory.insert_health_snapshot!(
          entity_id: "consumer-123",
          entity_kind: :sink_consumer,
          status: :healthy,
          sampled_at: DateTime.add(DateTime.utc_now(), -1, :second)
        )

      waiting =
        HealthFactory.insert_health_snapshot!(
          entity_id: "consumer-789",
          entity_kind: :sink_consumer,
          status: :waiting,
          sampled_at: DateTime.utc_now()
        )

      not_healthy =
        HealthFactory.insert_health_snapshot!(
          entity_id: "slot-456",
          entity_kind: :postgres_replication_slot,
          status: :error,
          sampled_at: DateTime.utc_now()
        )

      test_pid = self()

      # Expect the mocked prometheus calls for healthy snapshot (sink)
      # Sink entities gets 4 status metrics: ok, warn, error, paused
      # Replication slots entities get 3 status metrics: ok, warn, error
      expect(PrometheusMock, :set_entity_health, 11, fn entity_type, entity_id, status, value ->
        send(test_pid, {:set, %{entity_type: entity_type, entity_id: entity_id, status: status, value: value}})
        :ok
      end)

      assert :ok = EntityHealthMetrics.report_entity_health(PrometheusMock)

      payload =
        for _ <- 1..11, into: [] do
          assert_receive {:set, payload}
          payload
        end

      assert_lists_equal(payload, [
        %{entity_type: "sink", entity_id: healthy.entity_id, status: "ok", value: 1},
        %{entity_type: "sink", entity_id: healthy.entity_id, status: "warn", value: 0},
        %{entity_type: "sink", entity_id: healthy.entity_id, status: "error", value: 0},
        %{entity_type: "sink", entity_id: healthy.entity_id, status: "paused", value: 0},
        %{entity_type: "sink", entity_id: waiting.entity_id, status: "ok", value: 1},
        %{entity_type: "sink", entity_id: waiting.entity_id, status: "warn", value: 0},
        %{entity_type: "sink", entity_id: waiting.entity_id, status: "error", value: 0},
        %{entity_type: "sink", entity_id: waiting.entity_id, status: "paused", value: 0},
        %{entity_type: "replication_slot", entity_id: not_healthy.entity_id, status: "ok", value: 0},
        %{entity_type: "replication_slot", entity_id: not_healthy.entity_id, status: "warn", value: 0},
        %{entity_type: "replication_slot", entity_id: not_healthy.entity_id, status: "error", value: 1}
      ])
    end
  end
end
