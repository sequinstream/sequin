defmodule Sequin.Repo.Migrations.RemoveLegacyEventSingletonTransformConfig do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute(
      """
      update #{@config_schema}.sink_consumers
      set
        sink = jsonb_set(sink, '{batch}', 'false'::jsonb),
        batch_size = 1
      where
        type = 'http_push'
        and account_id in (
          select id
          from #{@config_schema}.accounts
          where inserted_at <= '2024-11-06'
        )
      """,
      ""
    )
  end
end
