defmodule Sequin.Consumers.HttpPushSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  defmodule Via do
    @moduledoc """
    This struct allows an HTTP push sink to be routed to an SQS queue for delivery. We send a
    payload to SQS that includes all details needed to perform delivery.
    """

    use Ecto.Schema
    use TypedEctoSchema

    alias Sequin.Encrypted

    typed_embedded_schema do
      field :queue_url, :string
      field :region, :string
      field :access_key_id, :string
      field :secret_access_key, Encrypted.Field
      field :is_fifo, :boolean, default: false
    end

    def changeset(struct, params) do
      struct
      |> cast(params, [:queue_url, :region, :access_key_id, :secret_access_key, :is_fifo])
      |> validate_required([:queue_url, :region, :access_key_id, :secret_access_key])
      |> validate_queue_url()
      |> put_is_fifo()
    end

    defp validate_queue_url(changeset) do
      changeset
      |> validate_format(:queue_url, sqs_url_regex(),
        message: "must be a valid AWS SQS URL (https://sqs.<region>.amazonaws.com/<account-id>/<queue-name>)"
      )
      |> validate_length(:queue_url, max: 2000)
    end

    defp put_is_fifo(changeset) do
      is_fifo = changeset |> get_field(:queue_url) |> ends_with_fifo?()
      put_change(changeset, :is_fifo, is_fifo)
    end

    defp ends_with_fifo?(nil), do: false
    defp ends_with_fifo?(url), do: String.ends_with?(url, ".fifo")

    def sqs_url_regex do
      ~r/^https:\/\/sqs\.(?<region>[a-z0-9-]+)\.amazonaws\.com\/\d{12}\/[a-zA-Z0-9_-]+(?:\.fifo)?$/
    end
  end

  @derive {Jason.Encoder, only: [:http_endpoint_id, :http_endpoint_path]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:http_push], default: :http_push
    field :http_endpoint_id, :binary_id
    field :http_endpoint_path, :string
    field :http_endpoint, :map, virtual: true
    field :mode, Ecto.Enum, values: [:static, :dynamic], default: :static
    field :batch, :boolean, default: true
    embeds_one :via, Via, on_replace: :delete
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:http_endpoint_id, :http_endpoint_path, :batch])
    |> validate_required([:http_endpoint_id])
    |> validate_http_endpoint_path()
    |> maybe_put_via()
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

  defp maybe_put_via(%Ecto.Changeset{valid?: true} = changeset) do
    case via_config() do
      {:ok, via} when is_map(via) ->
        %{
          queue_url: queue_url,
          region: region,
          access_key_id: access_key_id,
          secret_access_key: secret_access_key
        } = via

        is_fifo = String.ends_with?(queue_url, ".fifo")

        changeset
        |> put_change(:via, %{
          queue_url: queue_url,
          region: region,
          access_key_id: access_key_id,
          secret_access_key: secret_access_key,
          is_fifo: is_fifo
        })
        |> cast_embed(:via)

      _ ->
        changeset
    end
  end

  defp maybe_put_via(changeset) do
    changeset
  end

  defp via_config do
    if Application.get_env(:sequin, :env) == :test do
      :sequin |> Sequin.ApplicationMock.get_env(__MODULE__) |> Keyword.fetch(:via)
    else
      :sequin |> Application.get_env(__MODULE__) |> Keyword.fetch(:via)
    end
  end
end
