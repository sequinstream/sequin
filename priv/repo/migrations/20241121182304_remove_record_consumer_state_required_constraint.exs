defmodule Sequin.Repo.Migrations.RemoveRecordConsumerStateRequiredConstraint do
  use Ecto.Migration

  @config_schema_prefix Application.compile_env!(:sequin, Sequin.Repo)[:config_schema_prefix]

  def change do
    drop constraint(:sink_consumers, :record_consumer_state_required,
           prefix: @config_schema_prefix
         )
  end
end
