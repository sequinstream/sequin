defmodule Sequin.Repo.Migrations.MigrateLegacyTransformsToTransforms do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:transforms, prefix: @config_schema) do
      add :description, :text
    end

    # Create default transforms for each account
    execute """
            INSERT INTO #{@config_schema}.transforms (id, name, description, transform, account_id, inserted_at, updated_at)
            SELECT
              gen_random_uuid(),
              'record-transform',
              'Extracts just the record from the Sequin message shape.',
              jsonb_build_object('type', 'path', 'path', 'record'),
              a.id,
              NOW(),
              NOW()
            FROM #{@config_schema}.accounts a
            ON CONFLICT (account_id, name) DO NOTHING;
            """,
            """
            """

    execute """
            INSERT INTO #{@config_schema}.transforms (id, name, description, transform, account_id, inserted_at, updated_at)
            SELECT
              gen_random_uuid(),
              'id-transform',
              'Extracts just the id column from the record. Useful if your destination only needs to know which records exist or change. Only works if your table has an id column.',
              jsonb_build_object('type', 'path', 'path', 'record.id'),
              a.id,
              NOW(),
              NOW()
            FROM #{@config_schema}.accounts a
            ON CONFLICT (account_id, name) DO NOTHING;
            """,
            """
            """

    # Update sink consumers with legacy_transform = 'record_only' to use the new transform
    execute """
            UPDATE #{@config_schema}.sink_consumers sc
            SET
              transform_id = t.id,
              legacy_transform = 'none'
            FROM #{@config_schema}.transforms t
            WHERE sc.legacy_transform = 'record_only'
            AND sc.type != 'redis'
            AND t.name = 'record-transform'
            AND t.account_id = sc.account_id;
            """,
            """
            """
  end
end
