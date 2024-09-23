defmodule Sequin.Repo.Migrations.CreateLocalTunnels do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Create enum type
    execute "create type local_tunnel_entity_kind as enum('http_endpoint', 'postgres_database')"

    # Create available_bastion_ports table
    create table(:available_bastion_ports, prefix: @config_schema) do
      add :port, :integer, null: false
    end

    create unique_index(:available_bastion_ports, [:port], prefix: @config_schema)

    # Insert available bastion ports
    execute """
    insert into #{@config_schema}.available_bastion_ports (port)
    select generate_series(10000, 65535)
    """

    # Create local_tunnels table
    create table(:local_tunnels, prefix: @config_schema) do
      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :description, :string
      add :bastion_port, :integer, null: false

      timestamps(type: :utc_datetime)
    end

    create index(:local_tunnels, [:account_id], prefix: @config_schema)

    # Create function to assign random bastion port
    execute """
    create or replace function #{@config_schema}.assign_random_bastion_port()
    returns trigger as $$
    declare
      assigned_port integer;
    begin
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

      NEW.bastion_port := assigned_port;
      return NEW;
    end;
    $$ language plpgsql;
    """

    # Create trigger
    execute """
    create trigger assign_bastion_port_trigger
    before insert on #{@config_schema}.local_tunnels
    for each row
    execute function #{@config_schema}.assign_random_bastion_port();
    """

    alter table(:http_endpoints, prefix: @config_schema) do
      add :local_tunnel_id,
          references(:local_tunnels, on_delete: :delete_all, prefix: @config_schema)
    end

    alter table(:postgres_databases, prefix: @config_schema) do
      add :local_tunnel_id,
          references(:local_tunnels, on_delete: :delete_all, prefix: @config_schema)
    end
  end

  def down do
    alter table(:postgres_databases, prefix: @config_schema) do
      remove :local_tunnel_id
    end

    alter table(:http_endpoints, prefix: @config_schema) do
      remove :local_tunnel_id
    end

    # Drop trigger
    execute "drop trigger assign_bastion_port_trigger on #{@config_schema}.local_tunnels"

    # Drop function
    execute "drop function #{@config_schema}.assign_random_bastion_port()"

    # Drop local_tunnels table
    drop table(:local_tunnels, prefix: @config_schema)

    # Delete data and drop available_bastion_ports table
    execute "delete from #{@config_schema}.available_bastion_ports"
    drop table(:available_bastion_ports, prefix: @config_schema)

    # Drop enum type
    execute "drop type local_tunnel_entity_kind"
  end
end
