defmodule Sequin.Health do
  @moduledoc """
  This module tracks the health of various entities in the system.

  Notably, this module does not track the health of `%Resource{}` entities. See `Ix.SyncHealth`.
  """

  use TypedStruct

  alias __MODULE__
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health.Check
  alias Sequin.JSON
  alias Sequin.Replication.PostgresReplicationSlot

  @type status :: :healthy | :warning | :error | :initializing
  @type entity ::
          HttpEndpoint.t()
          | HttpPullConsumer.t()
          | HttpPushConsumer.t()
          | PostgresDatabase.t()
          | PostgresReplicationSlot.t()

  @derive Jason.Encoder
  typedstruct do
    field :org_id, String.t()
    field :entity_id, String.t()

    field :entity_kind,
          :http_endpoint
          | :http_pull_consumer
          | :http_push_consumer
          | :postgres_database
          | :postgres_replication_slot

    field :status, status()
    field :checks, %{required(String.t()) => Check.t()}
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
    |> JSON.decode_map_of_structs("checks", Check)
    |> JSON.struct(Health)
  end

  defguard is_entity(entity)
           when is_struct(entity, HttpEndpoint) or
                  is_struct(entity, HttpPullConsumer) or
                  is_struct(entity, HttpPushConsumer) or
                  is_struct(entity, PostgresDatabase) or
                  is_struct(entity, PostgresReplicationSlot)

  @subject_prefix "sequin-health"

  def subject_prefix, do: @subject_prefix

  @doc """
  Updates the `Health` of the given entity using the given `Check`.

  Generates one or more `:nats` messages to notify subscribers of the new status and any status changes.
  """
  @spec update(entity(), Check.t()) :: {:ok, Health.t()} | {:error, Error.t()}
  def update(entity, %Check{} = check) when is_entity(entity) do
    old_health =
      case get_health(entity.id) do
        {:ok, health} -> health
        {:error, %NotFoundError{}} -> nil
      end

    new_health = update_health_with_check(entity, old_health, check)

    with {:ok, new_health} <- set_health(entity.id, new_health) do
      publish(entity, new_health)

      if is_nil(old_health) or old_health.status != new_health.status do
        publish_status_change(entity, new_health)
      end

      {:ok, new_health}
    end
  end

  @doc """
  Retrieves the `Health` of the given entity.
  """
  @spec get(entity() | String.t()) :: {:ok, Health.t()} | {:error, Error.t()}
  def get(entity) when is_entity(entity) do
    get_health(entity.id)
  end

  def get(id) do
    get_health(id)
  end

  @doc """
  Subscribes to health updates for the given entity.

  Subscribers will receive a message every time a health updates.
  """
  @spec sub(entity()) :: {:ok, non_neg_integer()} | {:ok, String.t()} | {:error, String.t()}
  @spec sub(String.t()) :: {:ok, non_neg_integer()} | {:ok, String.t()} | {:error, String.t()}
  @spec sub(String.t(), Keyword.t()) :: {:ok, non_neg_integer()} | {:ok, String.t()} | {:error, String.t()}
  def sub(entity) when is_entity(entity) do
    Gnat.sub(:nats, self(), health_subject(entity.id))
  end

  @doc """
  Subscribes to health updates for the given organization.

  `opts` can optionally filter on other properties of the entity, such as `entity_kind` and `entity_id`.
  """
  def sub(org_id, opts \\ []) do
    Gnat.sub(:nats, self(), org_health_subject(org_id, opts))
  end

  @doc """
  Subscribes to health status changes for the given entity.

  Subscribers will receive a message every time a health status changes, ie. from `:healthy` to `:error`.
  """
  @spec sub_to_status_changes(entity()) :: {:ok, non_neg_integer()} | {:ok, String.t()} | {:error, String.t()}
  @spec sub_to_status_changes(String.t()) :: {:ok, non_neg_integer()} | {:ok, String.t()} | {:error, String.t()}
  def sub_to_status_changes(entity) when is_entity(entity) do
    Gnat.sub(:nats, self(), status_change_subject(entity.id))
  end

  def sub_to_status_changes(entity_id) do
    Gnat.sub(:nats, self(), status_change_subject(entity_id))
  end

  @doc """
  Subscribes to health status changes for the given organization.

  `opts` can optionally filter on other properties of the entity, such as `entity_kind` and `entity_id`.
  """
  @spec sub_to_org_status_changes(String.t(), Keyword.t()) ::
          {:ok, non_neg_integer()} | {:ok, String.t()} | {:error, String.t()}
  def sub_to_org_status_changes(org_id, opts \\ []) do
    Gnat.sub(:nats, self(), org_status_change_subject(org_id, opts))
  end

  ##############
  ## Internal ##
  ##############

  @spec add_check(t(), Check.t()) :: t()
  def add_check(%Health{} = health, %Check{} = check) do
    updated_checks = Map.put(health.checks, check.id, check)
    %Health{health | checks: updated_checks}
  end

  defp update_health_with_check(entity, nil, %Check{status: :error} = check) do
    %Health{
      org_id: entity.org_id,
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: :error,
      checks: %{check.id => check},
      erroring_since: DateTime.utc_now(),
      consecutive_errors: 1
    }
  end

  defp update_health_with_check(entity, nil, %Check{} = check) do
    %Health{
      org_id: entity.org_id,
      entity_id: entity.id,
      entity_kind: entity_kind(entity),
      status: check.status,
      checks: %{check.id => check},
      last_healthy_at: DateTime.utc_now(),
      erroring_since: nil,
      consecutive_errors: 0
    }
  end

  defp update_health_with_check(_entity, %Health{status: :error} = health, %Check{status: :error} = check) do
    %Health{health | checks: Map.put(health.checks, check.id, check), consecutive_errors: health.consecutive_errors + 1}
  end

  defp update_health_with_check(_entity, %Health{status: :error} = health, %Check{} = check) do
    %Health{
      health
      | status: check.status,
        checks: Map.put(health.checks, check.id, check),
        last_healthy_at: DateTime.utc_now(),
        erroring_since: nil,
        consecutive_errors: 0
    }
  end

  defp update_health_with_check(_entity, %Health{} = health, %Check{status: :error} = check) do
    %Health{
      health
      | status: check.status,
        checks: Map.put(health.checks, check.id, check),
        erroring_since: DateTime.utc_now(),
        consecutive_errors: 1
    }
  end

  defp update_health_with_check(_entity, %Health{} = health, %Check{} = check) do
    %Health{
      health
      | status: check.status,
        checks: Map.put(health.checks, check.id, check),
        last_healthy_at: DateTime.utc_now(),
        erroring_since: nil,
        consecutive_errors: 0
    }
  end

  defp publish(entity, %Health{} = health) when is_entity(entity) do
    # Gnat.pub(:nats, health_subject(entity), Jason.encode!(health))
    # TODO: Implement this
    :ok
  end

  defp publish_status_change(entity, %Health{} = health) when is_entity(entity) do
    # Gnat.pub(:nats, status_change_subject(entity), Jason.encode!(health))
    # TODO: Implement this
    :ok
  end

  defp entity_kind(%HttpEndpoint{}), do: :http_endpoint
  defp entity_kind(%HttpPullConsumer{}), do: :http_pull_consumer
  defp entity_kind(%HttpPushConsumer{}), do: :http_push_consumer
  defp entity_kind(%PostgresDatabase{}), do: :postgres_database
  defp entity_kind(%PostgresReplicationSlot{}), do: :postgres_replication_slot

  defp health_subject(entity) when is_entity(entity) do
    "#{@subject_prefix}.#{entity.org_id}.#{entity_kind(entity)}.#{entity.id}.health"
  end

  defp health_subject(entity_id) do
    "#{@subject_prefix}.*.*.#{entity_id}.health"
  end

  defp org_health_subject(org_id, opts) do
    entity_kind = opts[:entity_kind] || "*"
    entity_id = opts[:entity_id] || "*"

    "#{@subject_prefix}.#{org_id}.#{entity_kind}.#{entity_id}.health"
  end

  defp status_change_subject(entity) when is_entity(entity) do
    "#{@subject_prefix}.#{entity.org_id}.#{entity_kind(entity)}.#{entity.id}.status_change"
  end

  defp status_change_subject(entity_id) do
    "#{@subject_prefix}.*.*.#{entity_id}.status_change"
  end

  defp org_status_change_subject(org_id, opts) do
    entity_kind = opts[:entity_kind] || "*"
    entity_id = opts[:entity_id] || "*"

    "#{@subject_prefix}.#{org_id}.#{entity_kind}.#{entity_id}.status_change"
  end

  defp get_health(entity_id) do
    :redix
    |> Redix.command(["GET", "ix:health:v0:#{entity_id}"])
    |> case do
      {:ok, nil} -> {:error, Error.not_found(entity: :health)}
      {:ok, json} -> {:ok, Health.from_json!(json)}
      {:error, error} -> {:error, Error.service(entity: :redis, details: %{error: error})}
    end
  end

  defp set_health(entity_id, %Health{} = health) do
    :redix
    |> Redix.command(["SET", "ix:health:v0:#{entity_id}", Jason.encode!(health)])
    |> case do
      {:ok, "OK"} -> {:ok, health}
      {:error, error} -> {:error, Error.service(entity: :redis, details: %{error: error})}
    end
  end

  @doc """
  Converts a Health struct to a map with only the necessary fields for the frontend.
  """
  @spec to_external(t()) :: map()
  def to_external(%Health{} = health) do
    %{
      entity_kind: health.entity_kind,
      entity_id: health.entity_id,
      status: health.status,
      checks:
        Map.new(health.checks, fn {id, check} ->
          {id,
           %{
             name: check.name,
             status: check.status,
             error: if(check.error, do: %{message: Exception.message(check.error)})
           }}
        end)
    }
  end
end
