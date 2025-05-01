defmodule Sequin.Repo.Migrations.AddBatchToWebhookSinks do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  def change do
    execute(
      """
      update #{@config_schema}.sink_consumers
      set sink = jsonb_set(sink, '{batch}', 'true'::jsonb)
      where type = 'http_push'
      """,
      ""
    )

    execute(
      """
      alter table #{@config_schema}.sink_consumers
      add constraint ensure_batch_size_one
      check (not (type = 'http_push' and (sink->>'batch')::boolean = false) or batch_size = 1)
      """,
      """
      alter table #{@config_schema}.sink_consumers
      drop constraint ensure_batch_size_one
      """
    )
  end
end
