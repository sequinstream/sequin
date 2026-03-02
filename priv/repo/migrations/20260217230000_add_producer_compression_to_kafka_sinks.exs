defmodule Sequin.Repo.Migrations.AddProducerCompressionToKafkaSinks do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute(
      """
      update #{@config_schema}.sink_consumers
      set sink = jsonb_set(sink, '{producer_compression}', '"no_compression"'::jsonb)
      where type = 'kafka'
      """,
      ""
    )
  end
end
