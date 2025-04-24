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

    execute """
            alter table #{@config_schema}.sink_consumers
            add constraint routing_id_points_to_routing_transform
            check (
              routing_id is null or
              exists (
                select 1
                from #{@config_schema}.transforms
                where transforms.id = sink_consumers.routing_id
                  and transforms.type = 'routing'
              )
            )
            """,
            """
            alter table #{@config_schema}.sink_consumers
            drop constraint routing_id_points_to_routing_transform
            """
  end
end
