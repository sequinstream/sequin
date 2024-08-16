defmodule SequinWeb.PostgresReplicationJSON do
  @moduledoc false
  alias Sequin.Replication.PostgresReplicationSlot

  def render("index.json", %{postgres_replications: postgres_replications}) do
    %{data: postgres_replications}
  end

  def render("show.json", %{postgres_replication: postgres_replication}) do
    postgres_replication
  end

  def render("show_with_info.json", %{postgres_replication: %PostgresReplicationSlot{} = pgr}) do
    %{
      postgres_replication: pgr,
      info: pgr.info
    }
  end

  def render("delete.json", %{postgres_replication: postgres_replication}) do
    %{id: postgres_replication.id, deleted: true}
  end

  def render("backfill_jobs.json", %{job_ids: job_ids}) do
    %{job_ids: job_ids}
  end
end
