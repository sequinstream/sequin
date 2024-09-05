defmodule Sequin.Health do
  @moduledoc """
  This module tracks the health of various entities in the system.

  Notably, this module does not track the health of `%Resource{}` entities. See `Ix.SyncHealth`.
  """

  use TypedStruct

  import Sequin.Error.Guards, only: [is_error: 1]

  alias __MODULE__
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health.Check
  alias Sequin.JSON

  @type status :: :healthy | :warning | :error | :initializing
  @type entity ::
          HttpEndpoint.t()
          | HttpPullConsumer.t()
          | HttpPushConsumer.t()
          | PostgresDatabase.t()

  @derive Jason.Encoder
  typedstruct do
    field :org_id, String.t()
    field :entity_id, String.t()
    field :name, String.t()

    field :entity_kind,
          :http_endpoint
          | :http_pull_consumer
          | :http_push_consumer
          | :postgres_database

    field :status, status()
    field :checks, [Check.t()]
    field :last_healthy_at, DateTime.t() | nil
    field :erroring_since, DateTime.t() | nil
    field :consecutive_errors, non_neg_integer()
  end

  @spec from_json!(String.t()) :: t()
  def from_json!(json) when is_binary(json) do
    json
    |> Jason.decode!()
    |> JSON.decode_atom("status")
    |> JSON.decode_atom("entity_kind")
    |> JSON.decode_timestamp("last_healthy_at")
    |> JSON.decode_timestamp("erroring_since")
    |> JSON.decode_list_of_structs("checks", Check)
    |> JSON.struct(Health)
  end

  defguard is_entity(entity)
           when is_struct(entity, HttpEndpoint) or
                  is_struct(entity, HttpPullConsumer) or
                  is_struct(entity, HttpPushConsumer) or
                  is_struct(entity, PostgresDatabase)

  @subject_prefix "sequin-health"
  @debounce_window :timer.seconds(10)

  def subject_prefix, do: @subject_prefix
  def debounce_ets_table, do: :sequin_health_debounce

  @doc """
  Updates the `Health` of the given entity using the given `Check`.

  Generates one or more `:nats` messages to notify subscribers of the new status and any status changes.
  """
  @spec update(entity(), atom(), status(), Error.t() | nil) :: {:ok, Health.t()} | {:error, Error.t()}
  def update(entity, check_id, status, error \\ nil)

  def update(entity, check_id, status, error) when is_entity(entity) do
    validate_status_and_error!(status, error)

    key = "#{entity.id}:#{check_id}"
    now = :os.system_time(:millisecond)

    case :ets.lookup(:sequin_health_debounce, key) do
      [{^key, ^status, last_update}] when now - last_update < @debounce_window ->
        get_health(entity)

      _ ->
        :ets.insert(:sequin_health_debounce, {key, status, now})
        do_update(entity, check_id, status, error)
    end
  end

  defp do_update(entity, check_id, status, error) do
    with {:ok, old_health} <- get_health(entity) do
      %Check{} = expected_check = expected_check(entity, check_id, status, error)
      new_health = update_health_with_check(old_health, expected_check)
      set_health(entity.id, new_health)
    end
  end

  @doc """
  Retrieves the `Health` of the given entity.
  """
  @spec get(entity() | String.t()) :: {:ok, Health.t()} | {:error, Error.t()}
  def get(entity) when is_entity(entity) do
    get_health(entity)
  end

  #####################
  ## Expected Checks ##
  #####################

  defp expected_check(entity, check_id, status, error \\ nil)

  @postgres_checks [:reachable, :replication_connected, :replication_messages]
  defp expected_check(%PostgresDatabase{}, check_id, status, error) when check_id in @postgres_checks do
    case check_id do
      :reachable ->
        %Check{id: :reachable, name: "Database Reachable", status: status, error: error}

      :replication_connected ->
        %Check{id: :replication_connected, name: "Replication Connected", status: status, error: error}

      :replication_messages ->
        %Check{id: :replication_messages, name: "Replication Messages", status: status, error: error}
    end
  end

  @http_endpoint_checks [:reachable]
  defp expected_check(%HttpEndpoint{}, check_id, status, error) when check_id in @http_endpoint_checks do
    case check_id do
      :reachable -> %Check{id: :reachable, name: "Endpoint Reachable", status: status, error: error}
    end
  end

  @http_push_consumer_checks [:filters, :ingestion, :receive, :push, :acknowledge]
  defp expected_check(%HttpPushConsumer{}, check_id, status, error) when check_id in @http_push_consumer_checks do
    case check_id do
      :filters -> %Check{id: :filters, name: "Filters", status: status, error: error}
      :ingestion -> %Check{id: :ingestion, name: "Ingest", status: status, error: error}
      :receive -> %Check{id: :receive, name: "Produce", status: status, error: error}
      :push -> %Check{id: :push, name: "Push", status: status, error: error}
      :acknowledge -> %Check{id: :acknowledge, name: "Acknowledge", status: status, error: error}
    end
  end

  @http_pull_consumer_checks [:filters, :ingestion, :receive, :acknowledge]
  defp expected_check(%HttpPullConsumer{}, check_id, status, error) when check_id in @http_pull_consumer_checks do
    case check_id do
      :filters -> %Check{id: :filters, name: "Filters", status: status, error: error}
      :ingestion -> %Check{id: :ingestion, name: "Ingest", status: status, error: error}
      :receive -> %Check{id: :receive, name: "Pull", status: status, error: error}
      :acknowledge -> %Check{id: :acknowledge, name: "Acknowledge", status: status, error: error}
    end
  end

  #####################
  ## Initial Health ##
  #####################
  defp initial_health(%HttpPushConsumer{} = entity) do
    checks =
      @http_push_consumer_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :receive} = check ->
          %{check | message: "Whether the consumer is producing messages."}

        %Check{id: :push} = check ->
          %{check | message: "Pushing messages to your endpoint via HTTP."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "Consumer health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%HttpPullConsumer{} = entity) do
    checks =
      @http_pull_consumer_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :receive} = check ->
          %{check | message: "Pull messages from the consumer via HTTP."}

        %Check{id: :acknowledge} = check ->
          %{check | message: "Acknowledge messages via HTTP."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "Consumer health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%PostgresDatabase{} = entity) do
    checks =
      @postgres_checks
      |> Enum.map(&expected_check(entity, &1, :initializing))
      |> Enum.map(fn
        %Check{id: :replication_messages} = check ->
          %{check | message: "Messages will replicate when there is a change in your database."}

        %Check{} = check ->
          check
      end)

    %Health{
      name: "Database health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(%HttpEndpoint{} = entity) do
    checks = Enum.map(@http_endpoint_checks, &expected_check(entity, &1, :initializing))

    %Health{
      name: "Endpoint health",
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :initializing,
      checks: checks,
      consecutive_errors: 0
    }
  end

  defp initial_health(entity) when is_entity(entity) do
    raise "Not implemented for #{entity_kind(entity)}"
  end

  ##############
  ## Internal ##
  ##############

  defp update_health_with_check(%Health{} = health, %Check{} = check) do
    new_checks = replace_check_in_list(health.checks, check)
    new_status = calculate_overall_status(new_checks)

    %Health{
      health
      | status: new_status,
        checks: new_checks,
        last_healthy_at: if(new_status == :healthy, do: DateTime.utc_now(), else: health.last_healthy_at),
        erroring_since: if(new_status == :error, do: DateTime.utc_now(), else: health.erroring_since),
        consecutive_errors: if(new_status == :error, do: health.consecutive_errors + 1, else: 0)
    }
  end

  defp calculate_overall_status(checks) do
    checks
    |> Enum.map(& &1.status)
    |> Enum.min_by(&status_priority/1)
  end

  defp status_priority(status) do
    case status do
      :error -> 0
      :warning -> 1
      :initializing -> 2
      :healthy -> 3
    end
  end

  defp replace_check_in_list(checks, new_check) do
    Enum.map(checks, fn check ->
      if check.id == new_check.id, do: new_check, else: check
    end)
  end

  defp entity_kind(%HttpEndpoint{}), do: :http_endpoint
  defp entity_kind(%HttpPullConsumer{}), do: :http_pull_consumer
  defp entity_kind(%HttpPushConsumer{}), do: :http_push_consumer
  defp entity_kind(%PostgresDatabase{}), do: :postgres_database

  defp get_health(%{id: nil} = entity) when is_entity(entity) do
    raise ArgumentError, "entity_id cannot be nil"
  end

  defp get_health(entity) when is_entity(entity) do
    :redix
    |> Redix.command(["GET", "ix:health:v0:#{entity.id}"])
    |> case do
      {:ok, nil} -> {:ok, initial_health(entity)}
      {:ok, json} -> {:ok, Health.from_json!(json)}
      {:error, error} -> {:error, to_service_error(error)}
    end
  end

  defp set_health(entity_id, %Health{} = health) do
    :redix
    |> Redix.command(["SET", "ix:health:v0:#{entity_id}", Jason.encode!(health)])
    |> case do
      {:ok, "OK"} -> {:ok, health}
      {:error, error} -> {:error, to_service_error(error)}
    end
  end

  defp to_service_error(error) when is_exception(error) do
    Error.service(service: :redis, message: Exception.message(error))
  end

  defp to_service_error(error) do
    Error.service(service: :redis, message: "Redis error: #{inspect(error)}")
  end

  defp validate_status_and_error!(:healthy, nil), do: :ok
  defp validate_status_and_error!(:initializing, nil), do: :ok
  defp validate_status_and_error!(:warning, nil), do: :ok
  defp validate_status_and_error!(:warning, error) when is_error(error), do: :ok
  defp validate_status_and_error!(:error, error) when is_error(error), do: :ok

  defp validate_status_and_error!(:error, _), do: raise(ArgumentError, "error must be an Error struct for :error status")
  defp validate_status_and_error!(status, _), do: raise(ArgumentError, "Unexpected status: #{status}")

  @doc """
  Converts a Health struct to a map with only the necessary fields for the frontend.
  """
  @spec to_external(t()) :: map()
  def to_external(%Health{} = health) do
    %{
      entity_kind: health.entity_kind,
      entity_id: health.entity_id,
      status: health.status,
      name: health.name,
      checks:
        Enum.map(health.checks, fn check ->
          %{
            name: check.name,
            status: check.status,
            error: if(check.error, do: %{message: Exception.message(check.error)}),
            message: check.message
          }
        end)
    }
  end
end
