defmodule SequinWeb.PostgresReplicationJSON do
  @moduledoc false
  alias Sequin.Sources.PostgresReplication

  def render("index.json", %{postgres_replications: postgres_replications}) do
    %{data: postgres_replications}
  end

  def render("show.json", %{postgres_replication: postgres_replication}) do
    postgres_replication
  end

  def render("show_with_info.json", %{postgres_replication: %PostgresReplication{} = pgr}) do
    # Hack to show backfilling for a little bit longer on fast backfills
    postgres_replication =
      if pgr.status == :active and
           pgr.backfill_completed_at &&
           DateTime.diff(DateTime.utc_now(), pgr.backfill_completed_at) <= 5 do
        %{pgr | status: :backfilling, backfill_completed_at: nil}
      else
        pgr
      end

    %{
      postgres_replication: postgres_replication,
      info: postgres_replication.info
    }
  end

  def render("delete.json", %{postgres_replication: postgres_replication}) do
    %{id: postgres_replication.id, deleted: true}
  end

  def render("backfill_jobs.json", %{job_ids: job_ids}) do
    %{job_ids: job_ids}
  end
end
