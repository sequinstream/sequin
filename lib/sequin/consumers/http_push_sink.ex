defmodule Sequin.Consumers.HttpPushSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:http_endpoint_id, :http_endpoint_path]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:http_push], default: :http_push
    field :http_endpoint_id, :binary_id
    field :http_endpoint_path, :string
    field :http_endpoint, :map, virtual: true
    field :mode, Ecto.Enum, values: [:static, :dynamic], default: :static
    field :batch, :boolean, default: true
    field :via_sqs, :boolean, default: false
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:http_endpoint_id, :http_endpoint_path, :batch, :via_sqs])
    |> validate_required([:http_endpoint_id])
    |> validate_http_endpoint_path()
    |> maybe_set_via_sqs()
  end

  defp validate_http_endpoint_path(changeset) do
    changeset
    |> validate_format(
      :http_endpoint_path,
      ~r/^([\/\?\#]|$)/,
      message: "must start with '/', '?', '#', or be blank"
    )
    |> then(fn changeset ->
      if changeset.valid? do
        validate_format(
          changeset,
          :http_endpoint_path,
          ~r/^(\/[a-zA-Z0-9\-._~!$&'()*+,;=:@%?\/]*)?$/,
          message: "must be a valid URL path or empty"
        )
      else
        changeset
      end
    end)
    |> validate_length(:http_endpoint_path, max: 2000)
  end

  defp maybe_set_via_sqs(%Ecto.Changeset{valid?: true} = changeset) do
    # If via_sqs is not explicitly set in params, check the environment config
    if get_change(changeset, :via_sqs) == nil && via_enabled?() do
      put_change(changeset, :via_sqs, true)
    else
      changeset
    end
  end

  defp maybe_set_via_sqs(changeset) do
    changeset
  end

  defp via_enabled? do
    :sequin |> Sequin.get_env(__MODULE__) |> Keyword.get(:via_sqs_for_new_sinks?)
  end
end
