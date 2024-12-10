defmodule Sequin.Repo.Migrations.DropOrphanHttpEndpointsWithTrigger do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Create the trigger function
    execute("""
    create or replace function #{@config_schema}.delete_orphan_http_endpoint()
    returns trigger as $$
    begin
      -- Delete http_endpoint if this was the last sink_consumer referencing it
      delete from #{@config_schema}.http_endpoints
      where id = (old.sink->>'http_endpoint_id')::uuid
      and not exists (
        select 1
        from #{@config_schema}.sink_consumers
        where type = 'http_push'
        and (sink->>'http_endpoint_id')::uuid = (old.sink->>'http_endpoint_id')::uuid
      );
      return old;
    end;
    $$ language plpgsql;
    """)

    # Create the trigger
    execute("""
    create trigger delete_orphan_http_endpoint_after_consumer_delete
    after delete on #{@config_schema}.sink_consumers
    for each row
    when (old.type = 'http_push')
    execute function #{@config_schema}.delete_orphan_http_endpoint();
    """)
  end

  def down do
    execute(
      "drop trigger if exists delete_orphan_http_endpoint_after_consumer_delete on #{@config_schema}.sink_consumers"
    )

    execute("drop function if exists #{@config_schema}.delete_orphan_http_endpoint")
  end
end
