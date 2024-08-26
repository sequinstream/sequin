defmodule Sequin.Health do
  @moduledoc """
  This is a temporary module to hold health data for the health dashboard.
  """

  @derive Jason.Encoder
  defstruct [:component, :status, :message, :checks]

  defmodule Check do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:name, :status, :message, :error]
  end
end

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
      socket
      |> assign(
        healthy: %Health{
          status: :healthy,
          message: "Your push consumer is healthy",
          checks: [
            %Check{
              name: "Database WAL replication",
              status: :healthy,
              message: "WAL replication is functioning normally"
            },
            %Check{
              name: "Consumer ingestion",
              status: :healthy,
              message: "Data ingestion is working as expected"
            },
            %Check{
              name: "Consumer push webhooks",
              status: :healthy,
              message: "Webhooks are being pushed successfully"
            },
            %Check{
              name: "HTTP endpoint reachability",
              status: :healthy,
              message: "HTTP endpoint is reachable"
            }
          ],
          component: "Push Consumer"
        }
      )
      |> assign(
        warning: %Health{
          status: :warning,
          message: "Your push consumer has some issues",
          checks: [
            %Check{
              name: "Database WAL replication",
              status: :healthy,
              message: "WAL replication is functioning normally"
            },
            %Check{
              name: "Consumer ingestion",
              status: :warning,
              message: "Data ingestion is slower than usual"
            },
            %Check{
              name: "Consumer push webhooks",
              status: :healthy,
              message: "Webhooks are being pushed successfully"
            },
            %Check{
              name: "HTTP endpoint reachability",
              status: :healthy,
              message: "HTTP endpoint is reachable"
            }
          ],
          component: "Push Consumer"
        }
      )
      |> assign(
        unhealthy: %Health{
          status: :unhealthy,
          message: "Your push consumer has some critical issues",
          checks: [
            %Check{
              name: "Database WAL replication",
              status: :healthy,
              message: "WAL replication is functioning normally"
            },
            %Check{
              name: "Consumer ingestion",
              status: :healthy,
              message: "Data ingestion is working as expected"
            },
            %Check{
              name: "Consumer push webhooks",
              status: :warning,
              message: "Webhooks are being pushed with delays"
            },
            %Check{
              name: "HTTP endpoint reachability",
              status: :unhealthy,
              message: "HTTP endpoint is unreachable",
              error:
                Error.service(
                  code: "HTTP_ENDPOINT_UNREACHABLE",
                  message: "Failed to connect to the HTTP endpoint",
                  service: :push_consumer,
                  details: "Connection timed out after 30 seconds"
                )
            }
          ],
          component: "Push Consumer"
        }
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
            unhealthy: @unhealthy
          }
        }
      />
    </div>
    """
  end
end
