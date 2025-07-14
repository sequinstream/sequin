defmodule Sequin.Repo.Migrations.AddEnrichmentToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :enrichment_id, references(:functions)
    end

    execute """
            create or replace function #{@config_schema}.check_enrichment_id_type()
            returns trigger as $$
            begin
              if new.enrichment_id is not null then
                if not exists (
                  select 1 from #{@config_schema}.functions
                  where id = new.enrichment_id and type = 'sql_enrichment'
                ) then
                  raise exception 'enrichment_id must reference a function with type ''sql_enrichment''';
                end if;
              end if;
              return new;
            end;
            $$ language plpgsql;
            """,
            """
            drop function if exists #{@config_schema}.check_enrichment_id_type() cascade;
            """

    execute """
            create trigger enforce_enrichment_id_function_type
            before insert or update on #{@config_schema}.sink_consumers
            for each row execute function #{@config_schema}.check_enrichment_id_type();
            """,
            """
            drop trigger if exists enforce_enrichment_id_function_type on #{@config_schema}.sink_consumers;
            """
  end
end
