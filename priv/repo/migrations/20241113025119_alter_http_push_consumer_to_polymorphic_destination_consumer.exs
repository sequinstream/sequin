defmodule Sequin.Repo.Migrations.AlterHttpPushConsumerToPolymorphicDestinationConsumer do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    # Add new destination column
    alter table(:http_push_consumers, prefix: @config_schema) do
      add :destination, :jsonb
      add :type, :string, generated: "ALWAYS AS (destination->>'type') STORED"
      modify :http_endpoint_id, :binary_id, null: true
    end

    # Populate the destination column with existing http push config
    execute """
            UPDATE #{@config_schema}.http_push_consumers
            SET destination = jsonb_build_object(
              'type', 'http_push',
              'http_endpoint_id', http_endpoint_id,
              'http_endpoint_path', http_endpoint_path
            )
            """,
            "select 1;"

    rename(table(:http_push_consumers, prefix: @config_schema),
      to: table(:destination_consumers, prefix: @config_schema)
    )
  end
end
