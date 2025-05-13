defmodule Sequin.Consumers.AzureEventHubSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted.Field, as: EncryptedField
  alias Sequin.Sinks.Azure.EventHub

  @derive {Jason.Encoder, only: [:namespace, :event_hub_name, :shared_access_key_name]}
  @derive {Inspect, except: [:shared_access_key]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:azure_event_hub], default: :azure_event_hub
    field :connection_id, :string
    field :namespace, :string
    field :event_hub_name, :string
    field :shared_access_key_name, :string
    field :shared_access_key, EncryptedField
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :namespace,
      :event_hub_name,
      :shared_access_key_name,
      :shared_access_key
    ])
    |> validate_required([:namespace, :event_hub_name, :shared_access_key_name, :shared_access_key])
    |> put_connection_id()
  end

  defp put_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end

  def event_hub_client(%__MODULE__{} = sink) do
    EventHub.new(%{
      namespace: sink.namespace,
      event_hub_name: sink.event_hub_name,
      shared_access_key_name: sink.shared_access_key_name,
      shared_access_key: sink.shared_access_key
    })
  end
end
