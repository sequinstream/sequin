defmodule Sequin.Repo.Migrations.AddTimestampFormatToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    execute "CREATE TYPE #{@config_schema}.timestamp_format AS ENUM ('iso8601', 'unix_microsecond')"

    alter table(:sink_consumers, prefix: @config_schema) do
      add :timestamp_format, :"#{@config_schema}.timestamp_format",
        default: "iso8601",
        null: false
    end
  end

  def down do
    alter table(:sink_consumers, prefix: @config_schema) do
      remove :timestamp_format
    end

    execute "DROP TYPE #{@config_schema}.timestamp_format"
  end
end
