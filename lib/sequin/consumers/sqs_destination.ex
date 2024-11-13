defmodule Sequin.Consumers.SqsDestination do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:queue_url, :region, :endpoint]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:sqs], default: :sqs
    field :queue_url, :string
    field :region, :string
    field :endpoint, :string
    field :access_key_id, Encrypted.Binary
    field :secret_access_key, Encrypted.Binary
    field :is_fifo, :boolean, default: false
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:queue_url, :region, :endpoint, :access_key_id, :secret_access_key, :is_fifo])
    |> validate_required([:queue_url, :region, :access_key_id, :secret_access_key])
    |> validate_queue_url()
    |> put_is_fifo()
  end

  defp validate_queue_url(changeset) do
    changeset
    |> validate_format(:queue_url, ~r/^https:\/\/sqs\.[a-z0-9-]+\.amazonaws\.com\/\d{12}\/[a-zA-Z0-9_-]+(?:\.fifo)?$/,
      message: "must be a valid AWS SQS URL (https://sqs.<region>.amazonaws.com/<account-id>/<queue-name>)"
    )
    |> validate_length(:queue_url, max: 2000)
  end

  defp put_is_fifo(changeset) do
    if changeset |> get_field(:queue_url) |> ends_with_fifo?() do
      put_change(changeset, :is_fifo, true)
    else
      changeset
    end
  end

  defp ends_with_fifo?(nil), do: false
  defp ends_with_fifo?(url), do: String.ends_with?(url, ".fifo")
end
