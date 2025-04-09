defmodule Sequin.Repo.Migrations.MakeHttpEndpointNamesUnique do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    # First, handle any existing duplicate names by appending a random hex
    execute """
            WITH duplicates AS (
              SELECT id, name, account_id,
                     ROW_NUMBER() OVER (PARTITION BY name, account_id ORDER BY inserted_at) as rn
              FROM #{@config_schema}.http_endpoints
            )
            UPDATE #{@config_schema}.http_endpoints he
            SET name = d.name || '_' || substr(md5(random()::text), 1, 8)
            FROM duplicates d
            WHERE he.id = d.id AND d.rn > 1;
            """,
            """
            """

    # Then create the unique index
    create unique_index(:http_endpoints, [:name, :account_id], prefix: @config_schema)
  end
end
