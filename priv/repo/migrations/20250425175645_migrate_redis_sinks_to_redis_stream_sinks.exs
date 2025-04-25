defmodule Sequin.Repo.Migrations.MigrateRedisSinksToRedisStreamSinks do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            UPDATE #{@config_schema}.sink_consumers SET sink = jsonb_set(sink, '{type}', '"redis_stream"') WHERE type = 'redis'
            """,
            """
            UPDATE #{@config_schema}.sink_consumers SET sink = jsonb_set(sink, '{type}', '"redis"') WHERE type = 'redis_stream'
            """
  end
end
