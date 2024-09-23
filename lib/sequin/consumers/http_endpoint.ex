defmodule Sequin.Consumers.HttpEndpoint do
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Sequin.Repo

  @derive {Jason.Encoder,
           only: [:id, :name, :base_url, :headers, :account_id, :local_tunnel_id, :inserted_at, :updated_at]}
  typed_schema "http_endpoints" do
    field :name, :string
    field :base_url, :string
    field :headers, :map, default: %{}
    field :encrypted_headers, Sequin.Encrypted.Map
    field :health, :map, virtual: true

    belongs_to :account, Sequin.Accounts.Account
    belongs_to :local_tunnel, Sequin.Accounts.LocalTunnel

    has_many :http_push_consumers, Sequin.Consumers.HttpPushConsumer

    timestamps()
  end

  def create_changeset(http_endpoint, attrs) do
    http_endpoint
    |> cast(attrs, [:name, :base_url, :headers, :encrypted_headers, :local_tunnel_id])
    |> validate_required([:name, :base_url])
    |> validate_base_url()
    |> foreign_key_constraint(:account_id)
  end

  def update_changeset(http_endpoint, attrs) do
    http_endpoint
    |> cast(attrs, [:name, :base_url, :headers, :encrypted_headers, :local_tunnel_id])
    |> validate_base_url()
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([http_endpoint: he] in query, where: he.account_id == ^account_id)
  end

  defp validate_base_url(changeset) do
    case get_change(changeset, :base_url) do
      nil ->
        changeset

      base_url ->
        case URI.parse(base_url) do
          %URI{scheme: scheme, host: host} when is_binary(scheme) and is_binary(host) ->
            changeset

          _ ->
            add_error(changeset, :base_url, "must be a valid URL with scheme and host (ie. https://example.com)")
        end
    end
  end

  defp base_query(query \\ HttpEndpoint) do
    from(he in query, as: :http_endpoint)
  end

  def base_url(%__MODULE__{} = endpoint) do
    endpoint = Repo.preload(endpoint, :local_tunnel)

    if endpoint.local_tunnel do
      "http://#{Application.fetch_env!(:sequin, :portal_hostname)}:#{endpoint.local_tunnel.bastion_port}"
    else
      endpoint.base_url
    end
  end
end
