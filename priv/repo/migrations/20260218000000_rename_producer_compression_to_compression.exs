defmodule Sequin.Repo.Migrations.RenameProducerCompressionToCompression do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    execute("""
    update #{@config_schema}.sink_consumers
    set sink = sink
      - 'producer_compression'
      || jsonb_build_object(
           'compression',
           case sink->>'producer_compression'
             when 'no_compression' then 'none'
             else sink->>'producer_compression'
           end
         )
    where type = 'kafka'
    and sink ? 'producer_compression'
    """)
  end

  def down do
    execute("""
    update #{@config_schema}.sink_consumers
    set sink = sink
      - 'compression'
      || jsonb_build_object(
           'producer_compression',
           case sink->>'compression'
             when 'none' then 'no_compression'
             else sink->>'compression'
           end
         )
    where type = 'kafka'
    and sink ? 'compression'
    """)
  end
end
