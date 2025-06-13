defmodule Sequin.Runtime.Routing.Consumers.Nats do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:subject]}
  typed_embedded_schema do
    field :subject, :string
    field :headers, :map
  end

  def changeset(struct, params) do
    allowed_keys = [:subject, :headers]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:subject])
    |> validate_length(:subject, min: 1, max: 255)
    |> validate_format(:subject, ~r/^[a-zA-Z0-9\.\-_]+$/,
      message: "must contain only alphanumeric characters, dots, hyphens, and underscores"
    )
  end

  def route(action, _record, _changes, metadata) do
    %{
      subject: "sequin.#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.#{action}",
      headers: %{"Nats-Msg-Id" => metadata.idempotency_key}
    }
  end
end
