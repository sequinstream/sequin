defmodule SequinWeb.PostgresReplicationJSON do
  @moduledoc false

  def render("index.json", %{postgres_replications: postgres_replications}) do
    %{data: postgres_replications}
  end

  def render("show.json", %{postgres_replication: postgres_replication}) do
    postgres_replication
  end

  def render("show_with_info.json", %{postgres_replication: postgres_replication}) do
    %{
      postgres_replication: postgres_replication,
      info: postgres_replication.info
    }
  end

  def render("delete.json", %{postgres_replication: postgres_replication}) do
    %{id: postgres_replication.id, deleted: true}
  end
end
