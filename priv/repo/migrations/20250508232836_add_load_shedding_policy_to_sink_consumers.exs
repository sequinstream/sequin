defmodule Sequin.Repo.Migrations.AddLoadSheddingPolicyToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            create type #{@config_schema}.load_shedding_policy as enum('pause_on_full', 'discard_on_full');
            """,
            """
            drop type #{@config_schema}.load_shedding_policy;
            """

    alter table(:sink_consumers, prefix: @config_schema) do
      add :load_shedding_policy, :"#{@config_schema}.load_shedding_policy",
        null: false,
        default: "pause_on_full"
    end
  end
end
