defmodule Sequin.Repo.Migrations.DropLocalTunnels do
  use Ecto.Migration

  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Add `use_local_tunnel` column to `http_endpoints` and `postgres_databases`
    alter table(:http_endpoints, prefix: @config_schema) do
      add :use_local_tunnel, :boolean, default: false, null: false
    end

    alter table(:postgres_databases, prefix: @config_schema) do
      add :use_local_tunnel, :boolean, default: false, null: false
    end

    # Remove `local_tunnel_id` columns
    alter table(:http_endpoints, prefix: @config_schema) do
      remove :local_tunnel_id
      modify :scheme, :"#{@config_schema}.http_scheme", null: true
    end

    alter table(:postgres_databases, prefix: @config_schema) do
      remove :local_tunnel_id
    end

    # Drop `local_tunnels` table
    drop table(:local_tunnels, prefix: @config_schema)

    # Add constraints to allow `host` to be null when `use_local_tunnel` is true
    create constraint(:http_endpoints, :http_endpoints_host_not_null_check,
             check: "use_local_tunnel OR host IS NOT NULL OR scheme IS NOT NULL",
             prefix: @config_schema
           )

    create constraint(:postgres_databases, :postgres_databases_hostname_not_null_check,
             check: "use_local_tunnel OR hostname IS NOT NULL",
             prefix: @config_schema
           )

    # Create function to assign random bastion port
    execute """
    create or replace function #{@config_schema}.assign_random_bastion_port()
    returns trigger as $$
    declare
      assigned_port integer;
    begin
      if new.use_local_tunnel and (tg_op = 'INSERT' or (tg_op = 'UPDATE' and new.port is null)) then
        with random_port as (
          select port
          from #{@config_schema}.available_bastion_ports
          order by random()
          limit 1
          for update skip locked
        )
        delete from #{@config_schema}.available_bastion_ports
        where port = (select port from random_port)
        returning port into assigned_port;

        new.port := assigned_port;
      end if;
      return new;
    end;
    $$ language plpgsql;
    """

    # Add triggers to `http_endpoints` and `postgres_databases`
    execute """
    create trigger assign_bastion_port_http_endpoints
    before insert or update on #{@config_schema}.http_endpoints
    for each row
    execute function #{@config_schema}.assign_random_bastion_port();
    """

    execute """
    create trigger assign_bastion_port_postgres_databases
    before insert or update on #{@config_schema}.postgres_databases
    for each row
    execute function #{@config_schema}.assign_random_bastion_port();
    """
  end

  def down do
    # Remove triggers
    execute(
      "drop trigger if exists assign_bastion_port_http_endpoints on #{@config_schema}.http_endpoints;"
    )

    execute(
      "drop trigger if exists assign_bastion_port_postgres_databases on #{@config_schema}.postgres_databases;"
    )

    execute("drop function if exists #{@config_schema}.assign_random_bastion_port();")

    # Remove constraints
    drop constraint(:http_endpoints, :http_endpoints_host_not_null_check)
    drop constraint(:postgres_databases, :postgres_databases_hostname_not_null_check)

    # Recreate `local_tunnels` table
    create table(:local_tunnels, prefix: @config_schema) do
      add :bastion_port, :integer, null: false

      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      timestamps()
    end

    # Add `local_tunnel_id` columns back
    alter table(:http_endpoints, prefix: @config_schema) do
      add :local_tunnel_id,
          references(:local_tunnels, on_delete: :nilify_all, prefix: @config_schema)
    end

    alter table(:postgres_databases, prefix: @config_schema) do
      add :local_tunnel_id,
          references(:local_tunnels, on_delete: :nilify_all, prefix: @config_schema)
    end

    # Remove `use_local_tunnel` columns
    alter table(:http_endpoints, prefix: @config_schema) do
      remove :use_local_tunnel
    end

    alter table(:postgres_databases, prefix: @config_schema) do
      remove :use_local_tunnel
    end
  end
end
