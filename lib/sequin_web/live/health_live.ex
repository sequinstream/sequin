defmodule SequinWeb.HealthLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Check

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    socket = assign(socket, page_title: "Health Dashboard")

    socket =
      assign(socket,
        healthy:
          Health.to_external(%Health{
            entity_id: "consumer_1",
            entity_kind: :http_push_consumer,
            status: :healthy,
            checks: %{
              "wal_replication" => %Check{
                id: "wal_replication",
                name: "Database WAL replication",
                status: :healthy,
                error: nil
              },
              "ingestion" => %Check{id: "ingestion", name: "Consumer ingestion", status: :healthy, error: nil},
              "webhooks" => %Check{id: "webhooks", name: "Consumer push webhooks", status: :healthy, error: nil},
              "http_endpoint" => %Check{
                id: "http_endpoint",
                name: "HTTP endpoint reachability",
                status: :healthy,
                error: nil
              }
            },
            last_healthy_at: DateTime.utc_now(),
            erroring_since: nil,
            consecutive_errors: 0
          }),
        warning:
          Health.to_external(%Health{
            entity_id: "consumer_2",
            entity_kind: :http_push_consumer,
            status: :warning,
            checks: %{
              "wal_replication" => %Check{
                id: "wal_replication",
                name: "Database WAL replication",
                status: :healthy,
                error: nil
              },
              "ingestion" => %Check{
                id: "ingestion",
                name: "Consumer ingestion",
                status: :warning,
                error:
                  Error.service(
                    code: "SLOW_INGESTION",
                    message: "Data ingestion is slower than usual",
                    service: :push_consumer
                  )
              },
              "webhooks" => %Check{id: "webhooks", name: "Consumer push webhooks", status: :healthy, error: nil},
              "http_endpoint" => %Check{
                id: "http_endpoint",
                name: "HTTP endpoint reachability",
                status: :healthy,
                error: nil
              }
            },
            last_healthy_at: DateTime.add(DateTime.utc_now(), -1800, :second),
            erroring_since: DateTime.add(DateTime.utc_now(), -900, :second),
            consecutive_errors: 1
          }),
        unhealthy:
          Health.to_external(%Health{
            entity_id: "consumer_3",
            entity_kind: :http_push_consumer,
            status: :error,
            checks: %{
              "wal_replication" => %Check{
                id: "wal_replication",
                name: "Database WAL replication",
                status: :healthy,
                error: nil
              },
              "ingestion" => %Check{id: "ingestion", name: "Consumer ingestion", status: :healthy, error: nil},
              "webhooks" => %Check{
                id: "webhooks",
                name: "Consumer push webhooks",
                status: :error,
                error:
                  Error.service(
                    code: "WEBHOOK_DELAYS",
                    message: "Webhooks are being pushed with delays",
                    service: :push_consumer
                  )
              },
              "http_endpoint" => %Check{
                id: "http_endpoint",
                name: "HTTP endpoint reachability",
                status: :error,
                error:
                  Error.service(
                    code: "HTTP_ENDPOINT_UNREACHABLE",
                    message: "Failed to connect to the HTTP endpoint",
                    service: :push_consumer,
                    details: "Connection timed out after 30 seconds"
                  )
              }
            },
            last_healthy_at: DateTime.add(DateTime.utc_now(), -3600, :second),
            erroring_since: DateTime.add(DateTime.utc_now(), -1800, :second),
            consecutive_errors: 3
          }),
        initializing:
          Health.to_external(%Health{
            entity_id: "consumer_4",
            entity_kind: :http_push_consumer,
            status: :initializing,
            checks: %{
              "wal_replication" => %Check{
                id: "wal_replication",
                name: "Database WAL replication",
                status: :healthy,
                error: nil
              },
              "ingestion" => %Check{id: "ingestion", name: "Consumer ingestion", status: :initializing, error: nil},
              "webhooks" => %Check{id: "webhooks", name: "Consumer push webhooks", status: :initializing, error: nil},
              "http_endpoint" => %Check{
                id: "http_endpoint",
                name: "HTTP endpoint reachability",
                status: :initializing,
                error: nil
              }
            },
            last_healthy_at: nil,
            erroring_since: nil,
            consecutive_errors: 0
          })
      )

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :parent_id, "health-dashboard")

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="health/Grid"
        props={
          %{
            parent_id: @parent_id,
            healthy: @healthy,
            warning: @warning,
            unhealthy: @unhealthy,
            initializing: @initializing
          }
        }
      />
    </div>
    """
  end
end
