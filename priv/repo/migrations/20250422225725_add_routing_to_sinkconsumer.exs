defmodule Sequin.Repo.Migrations.AddRoutingToSinkconsumer do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            alter table #{@config_schema}.sink_consumers add column routing_id uuid references #{@config_schema}.transforms(id)
            """,
            """
            alter table #{@config_schema}.sink_consumers drop column routing_id
            """
  end
end
