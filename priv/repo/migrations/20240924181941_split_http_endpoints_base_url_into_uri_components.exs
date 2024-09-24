defmodule Sequin.Repo.Migrations.SplitHttpEndpointsBaseUrlIntoUriComponents do
  use Ecto.Migration

  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Create http_scheme type
    execute "CREATE TYPE #{@config_schema}.http_scheme AS ENUM ('http', 'https')"

    # Add new columns
    alter table(:http_endpoints, prefix: @config_schema) do
      add :scheme, :"#{@config_schema}.http_scheme"
      add :userinfo, :string
      add :host, :string
      add :port, :integer
      add :path, :string
      add :query, :string
      add :fragment, :string
    end

    # Migrate existing data
    execute """
    UPDATE #{@config_schema}.http_endpoints
    SET
      scheme = CASE
                 WHEN strpos(base_url, 'https://') = 1 THEN 'https'::#{@config_schema}.http_scheme
                 ELSE 'http'::#{@config_schema}.http_scheme
               END,
      host = substring(base_url FROM '://([^/]+)')::text,
      path = substring(base_url FROM '://[^/]+(/[^?]*)')::text,
      query = substring(base_url FROM '\\?(.*)')::text
    WHERE base_url IS NOT NULL
    """

    # Add not null constraint to scheme and host
    alter table(:http_endpoints, prefix: @config_schema) do
      modify :scheme, :"#{@config_schema}.http_scheme", null: false
      modify :host, :string
    end

    # Add constraint: host can be null only when local_tunnel_id is set
    create constraint(:http_endpoints, :host_required_when_no_local_tunnel,
             check: "(local_tunnel_id IS NOT NULL) OR (host IS NOT NULL)",
             prefix: @config_schema
           )

    # Drop base_url column
    alter table(:http_endpoints, prefix: @config_schema) do
      remove :base_url
    end
  end

  def down do
    # Add base_url column back
    alter table(:http_endpoints, prefix: @config_schema) do
      add :base_url, :string
    end

    # Migrate data back to base_url
    execute """
    update #{@config_schema}.http_endpoints
    set base_url = scheme || '://' || coalesce(host, '') || coalesce(path, '')
    """

    # Remove new columns
    alter table(:http_endpoints, prefix: @config_schema) do
      remove :scheme
      remove :userinfo
      remove :host
      remove :port
      remove :path
      remove :query
      remove :fragment
    end

    # Drop http_scheme type
    execute "DROP TYPE #{@config_schema}.http_scheme"
  end
end
