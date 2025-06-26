defmodule Sequin.Runtime.Routing.Consumers.HttpPush do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:method, :endpoint_path]}
  typed_embedded_schema do
    field :method, :string
    field :endpoint_path, :string
    field :headers, :map
  end

  def changeset(struct, params) do
    allowed_keys = [:method, :endpoint_path, :headers]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:method])
    |> validate_inclusion(:method, ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
      message: "is invalid, valid values are: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS"
    )
    |> validate_length(:endpoint_path, max: 2000)
  end

  def route(_action, _record, _changes, _metadata) do
    %{
      method: "POST",
      endpoint_path: "",
      headers: %{}
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    if sink.http_endpoint_path do
      %{endpoint_path: sink.http_endpoint_path}
    else
      %{endpoint_path: ""}
    end
  end
end
