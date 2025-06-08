defmodule Sequin.Sinks.Gcp.Credentials do
  @moduledoc """
  Schema for Google Cloud Platform credentials.
  Supports different authentication methods including service accounts,
  OAuth client credentials, API keys, etc.
  """
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted

  @derive {Inspect, except: [:private_key, :client_secret, :api_key, :private_key_id, :client_email]}
  @primary_key false
  typed_embedded_schema do
    field :type, :string
    field :project_id, :string

    # Service Account fields
    field :private_key_id, :string
    field :private_key, Encrypted.Field
    field :client_email, :string
    field :client_id, :string
    field :auth_uri, :string
    field :token_uri, :string
    field :auth_provider_x509_cert_url, :string
    field :client_x509_cert_url, :string
    field :universe_domain, :string

    # OAuth Client fields
    field :client_secret, Encrypted.Field

    # API Key field
    field :api_key, Encrypted.Field
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :type,
      :project_id,
      # Service Account fields
      :private_key_id,
      :private_key,
      :client_email,
      :client_id,
      :auth_uri,
      :token_uri,
      :auth_provider_x509_cert_url,
      :client_x509_cert_url,
      :universe_domain,
      # OAuth fields
      :client_secret,
      # API Key
      :api_key
    ])
    |> validate_required([:type, :project_id])
    |> validate_type_specific_fields()
  end

  defp validate_type_specific_fields(changeset) do
    case get_field(changeset, :type) do
      "service_account" ->
        changeset
        |> validate_required([
          :private_key_id,
          :private_key,
          :client_email,
          :client_id
        ])
        |> validate_format(:client_email, ~r/@.*\.iam\.gserviceaccount\.com$/)

      "oauth_client" ->
        validate_required(changeset, [:client_id, :client_secret])

      "api_key" ->
        validate_required(changeset, [:api_key])

      type when type in [nil, ""] ->
        add_error(changeset, :type, "is required")

      type ->
        add_error(changeset, :type, "#{type} is not a supported credential type")
    end
  end
end
