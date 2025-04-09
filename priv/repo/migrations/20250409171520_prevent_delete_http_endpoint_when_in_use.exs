defmodule Sequin.Repo.Migrations.PreventDeleteHttpEndpointWhenInUse do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            CREATE OR REPLACE FUNCTION #{@config_schema}.prevent_http_endpoint_deletion()
            RETURNS TRIGGER AS $$
            DECLARE
              in_use_count INTEGER;
            BEGIN
              SELECT COUNT(*) INTO in_use_count
              FROM #{@config_schema}.sink_consumers sc
              WHERE (sc.sink->>'type' = 'http_push' AND (sc.sink->>'http_endpoint_id')::uuid = OLD.id)
                AND sc.account_id = OLD.account_id;

              IF in_use_count > 0 THEN
                RAISE EXCEPTION 'Cannot delete HTTP endpoint % (%), as it is in use by % sink consumer(s)',
                  OLD.name, OLD.id, in_use_count;
              END IF;

              RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            DROP FUNCTION IF EXISTS #{@config_schema}.prevent_http_endpoint_deletion();
            """

    execute """
            CREATE TRIGGER prevent_http_endpoint_deletion
            BEFORE DELETE ON #{@config_schema}.http_endpoints
            FOR EACH ROW
            EXECUTE FUNCTION #{@config_schema}.prevent_http_endpoint_deletion();
            """,
            """
            DROP TRIGGER IF EXISTS prevent_http_endpoint_deletion ON #{@config_schema}.http_endpoints;
            """
  end
end
