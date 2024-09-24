defmodule Sequin.Consumers.HttpEndpoint do
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Sequin.Repo

  @derive {Jason.Encoder,
           only: [
             :id,
             :name,
             :scheme,
             :userinfo,
             :host,
             :port,
             :path,
             :query,
             :fragment,
             :headers,
             :account_id,
             :local_tunnel_id,
             :inserted_at,
             :updated_at
           ]}
  typed_schema "http_endpoints" do
    field :name, :string
    field :scheme, Ecto.Enum, values: [:http, :https]
    field :userinfo, :string
    field :host, :string
    field :port, :integer
    field :path, :string
    field :query, :string
    field :fragment, :string
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
    |> cast(attrs, [
      :name,
      :scheme,
      :userinfo,
      :host,
      :port,
      :path,
      :query,
      :fragment,
      :headers,
      :encrypted_headers,
      :local_tunnel_id
    ])
    |> validate_required([:name, :scheme, :host])
    |> validate_uri_components()
    |> foreign_key_constraint(:account_id)
  end

  def update_changeset(http_endpoint, attrs) do
    http_endpoint
    |> cast(attrs, [
      :name,
      :scheme,
      :userinfo,
      :host,
      :port,
      :path,
      :query,
      :fragment,
      :headers,
      :encrypted_headers,
      :local_tunnel_id
    ])
    |> validate_uri_components()
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([http_endpoint: he] in query, where: he.account_id == ^account_id)
  end

  defp validate_uri_components(changeset) do
    changeset
    |> validate_inclusion(:scheme, [:http, :https])
    |> validate_format(:host, ~r/^[a-zA-Z0-9.-]+$/, message: "must be a valid hostname")
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
  end

  defp base_query(query \\ HttpEndpoint) do
    from(he in query, as: :http_endpoint)
  end

  # If the URI is incomplete (when initiating a new endpoint), a partial URI would be nonsensical.
  def url(%__MODULE__{host: nil, path: nil}), do: ""

  def url(%__MODULE__{} = endpoint) do
    endpoint |> uri() |> URI.to_string()
  end

  def uri(%__MODULE__{} = endpoint) do
    endpoint = Repo.preload(endpoint, :local_tunnel)

    if endpoint.local_tunnel do
      %URI{
        scheme: "http",
        host: Application.fetch_env!(:sequin, :portal_hostname),
        port: endpoint.local_tunnel.bastion_port,
        path: endpoint.path,
        query: endpoint.query,
        fragment: endpoint.fragment
      }
    else
      port = endpoint.port || if endpoint.scheme in [:https, "https"], do: 443, else: 80

      %URI{
        scheme: to_string(endpoint.scheme),
        userinfo: endpoint.userinfo,
        host: endpoint.host,
        port: port,
        path: endpoint.path,
        query: endpoint.query,
        fragment: endpoint.fragment
      }
    end
  end
end
