defmodule SequinWeb.ConfigJSON do
  alias Ecto.Changeset
  alias Sequin.Accounts.User
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Replication.WalPipeline

  def render("plan.json", %{changesets: changesets}) do
    %{data: Enum.map(changesets, &render_changeset/1)}
  end

  @user_fields [:name, :email, :auth_provider, :hashed_password]
  defp render_changeset(%Changeset{data: %User{} = data} = changeset) do
    %{
      action: changeset.action,
      data: data |> Map.take(@user_fields) |> maybe_redact_field(:hashed_password),
      changes: changeset.changes |> Map.take(@user_fields) |> maybe_redact_field(:hashed_password),
      errors: changeset.errors
    }
  end

  @sequence_fields [:name, :table_oid, :sort_column_attnum, :sort_column_name]
  defp render_changeset(%Changeset{data: %Sequence{} = data} = changeset) do
    %{
      action: changeset.action,
      data: Map.take(data, @sequence_fields),
      changes: Map.take(changeset.changes, @sequence_fields),
      errors: changeset.errors
    }
  end

  @http_endpoint_fields [
    :name,
    :scheme,
    :userinfo,
    :host,
    :port,
    :path,
    :query,
    :fragment,
    :headers,
    :use_local_tunnel
  ]
  defp render_changeset(%Changeset{data: %HttpEndpoint{} = data} = changeset) do
    %{
      action: changeset.action,
      data: Map.take(data, @http_endpoint_fields),
      changes: Map.take(changeset.changes, @http_endpoint_fields),
      errors: changeset.errors
    }
  end

  @postgres_database_fields [
    :database,
    :hostname,
    :pool_size,
    :port,
    :queue_interval,
    :queue_target,
    :name,
    :ssl,
    :username,
    :password,
    :ipv6,
    :use_local_tunnel
  ]
  defp render_changeset(%Changeset{data: %PostgresDatabase{} = data} = changeset) do
    %{
      action: changeset.action,
      data: data |> Map.take(@postgres_database_fields) |> maybe_redact_field(:password),
      changes: changeset.changes |> Map.take(@postgres_database_fields) |> maybe_redact_field(:password),
      errors: changeset.errors
    }
  end

  @wal_pipeline_fields [
    :name,
    :status,
    :seq,
    :destination_oid,
    :replication_slot_id,
    :destination_database_id,
    :annotations
  ]
  defp render_changeset(%Changeset{data: %WalPipeline{} = data} = changeset) do
    %{
      action: changeset.action,
      data: Map.take(data, @wal_pipeline_fields),
      changes: Map.take(changeset.changes, @wal_pipeline_fields),
      errors: changeset.errors
    }
  end

  @http_push_consumer_fields [
    :name,
    :ack_wait_ms,
    :max_ack_pending,
    :max_deliver,
    :max_waiting,
    :message_kind,
    :status,
    :batch_size,
    :http_endpoint_id,
    :replication_slot_id,
    :sequence_id,
    :http_endpoint_path
  ]
  defp render_changeset(%Changeset{data: %HttpPushConsumer{} = data} = changeset) do
    %{
      action: changeset.action,
      data: Map.take(data, @http_push_consumer_fields),
      changes: Map.take(changeset.changes, @http_push_consumer_fields),
      errors: changeset.errors
    }
  end

  @http_pull_consumer_fields [
    :name,
    :ack_wait_ms,
    :max_ack_pending,
    :max_deliver,
    :max_waiting,
    :message_kind,
    :status,
    :replication_slot_id,
    :sequence_id
  ]
  defp render_changeset(%Changeset{data: %HttpPullConsumer{} = data} = changeset) do
    %{
      action: changeset.action,
      data: Map.take(data, @http_pull_consumer_fields),
      changes: Map.take(changeset.changes, @http_pull_consumer_fields),
      errors: changeset.errors
    }
  end

  defp maybe_redact_field(map, field) do
    if Map.has_key?(map, field) and not is_nil(Map.get(map, field)) do
      Map.put(map, field, "******")
    else
      map
    end
  end
end
