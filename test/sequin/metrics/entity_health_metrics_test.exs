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
          entity_name: "my-webhook-sink",
          entity_kind: :sink_consumer,
          status: :healthy,
          sampled_at: DateTime.add(DateTime.utc_now(), -1, :second)
        )

      waiting =
        HealthFactory.insert_health_snapshot!(
          entity_id: "consumer-789",
          entity_name: "my-other-sink",
          entity_kind: :sink_consumer,
          status: :waiting,
          sampled_at: DateTime.utc_now()
        )

      not_healthy =
        HealthFactory.insert_health_snapshot!(
          entity_id: "slot-456",
          entity_name: "my-slot",
          entity_kind: :postgres_replication_slot,
          status: :error,
          sampled_at: DateTime.utc_now()
        )

      test_pid = self()

      # Expect the mocked prometheus calls for health snapshots
      # Each entity gets 4 status metrics: ok, warn, error, paused (3 entities × 4 = 12)
      expect(PrometheusMock, :set_entity_health, 12, fn entity_type, entity_id, entity_name, status, value ->
        send(
          test_pid,
          {:set,
           %{entity_type: entity_type, entity_id: entity_id, entity_name: entity_name, status: status, value: value}}
        )

        :ok
      end)

      assert :ok = EntityHealthMetrics.report_entity_health(PrometheusMock)

      payload =
        for _ <- 1..12, into: [] do
          assert_receive {:set, payload}
          payload
        end

      assert_lists_equal(payload, [
        %{entity_type: "sink", entity_id: healthy.entity_id, entity_name: healthy.entity_name, status: "ok", value: 1},
        %{entity_type: "sink", entity_id: healthy.entity_id, entity_name: healthy.entity_name, status: "warn", value: 0},
        %{entity_type: "sink", entity_id: healthy.entity_id, entity_name: healthy.entity_name, status: "error", value: 0},
        %{
          entity_type: "sink",
          entity_id: healthy.entity_id,
          entity_name: healthy.entity_name,
          status: "paused",
          value: 0
        },
        %{entity_type: "sink", entity_id: waiting.entity_id, entity_name: waiting.entity_name, status: "ok", value: 1},
        %{entity_type: "sink", entity_id: waiting.entity_id, entity_name: waiting.entity_name, status: "warn", value: 0},
        %{entity_type: "sink", entity_id: waiting.entity_id, entity_name: waiting.entity_name, status: "error", value: 0},
        %{
          entity_type: "sink",
          entity_id: waiting.entity_id,
          entity_name: waiting.entity_name,
          status: "paused",
          value: 0
        },
        %{
          entity_type: "replication_slot",
          entity_id: not_healthy.entity_id,
          entity_name: not_healthy.entity_name,
          status: "ok",
          value: 0
        },
        %{
          entity_type: "replication_slot",
          entity_id: not_healthy.entity_id,
          entity_name: not_healthy.entity_name,
          status: "warn",
          value: 0
        },
        %{
          entity_type: "replication_slot",
          entity_id: not_healthy.entity_id,
          entity_name: not_healthy.entity_name,
          status: "error",
          value: 1
        },
        %{
          entity_type: "replication_slot",
          entity_id: not_healthy.entity_id,
          entity_name: not_healthy.entity_name,
          status: "paused",
          value: 0
        }
      ])
    end
  end
end
