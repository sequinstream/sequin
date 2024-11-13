defmodule Sequin.Consumers.HttpEndpoint do
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Ecto.Changeset

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
             :use_local_tunnel,
             :inserted_at,
             :updated_at
           ]}
  typed_schema "http_endpoints" do
    field :name, :string
    field :scheme, Ecto.Enum, values: [:http, :https]
    field :userinfo, :string
    field :host, :string
    # can be auto-generated when use_local_tunnel is true
    field :port, :integer, read_after_writes: true
    field :path, :string
    field :query, :string
    field :fragment, :string
    field :headers, :map, default: %{}
    field :encrypted_headers, Sequin.Encrypted.Map
    field :health, :map, virtual: true
    field :use_local_tunnel, :boolean, default: false

    belongs_to :account, Sequin.Accounts.Account

    has_many :destination_consumers, Sequin.Consumers.DestinationConsumer

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
      :use_local_tunnel
    ])
    |> validate_required([:name])
    |> validate_uri_components()
    |> foreign_key_constraint(:account_id)
    |> validate_no_port_if_local_tunnel_enabled()
    |> Sequin.Changeset.validate_name()
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
      :use_local_tunnel
    ])
    |> validate_uri_components()
    |> Sequin.Changeset.validate_name()
  end

  defp validate_uri_components(changeset) do
    changeset
    |> validate_inclusion(:scheme, [:http, :https])
    |> validate_format(:host, ~r/^[a-zA-Z0-9.-]+$/, message: "must be a valid hostname")
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
    |> maybe_validate_required()
  end

  defp maybe_validate_required(changeset) do
    if changeset.valid? && !Changeset.get_field(changeset, :use_local_tunnel) do
      validate_required(changeset, [:scheme, :host])
    else
      changeset
    end
  end

  defp validate_no_port_if_local_tunnel_enabled(changeset) do
    if changeset.valid? && Changeset.get_field(changeset, :use_local_tunnel) && Changeset.get_field(changeset, :port) do
      add_error(changeset, :port, "must not be set if local tunnel is enabled")
    else
      changeset
    end
  end

  def where_id(query \\ base_query(), id) do
    from([http_endpoint: he] in query, where: he.id == ^id)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([http_endpoint: he] in query, where: he.account_id == ^account_id)
  end

  def where_name(query \\ base_query(), name) do
    from([http_endpoint: he] in query, where: he.name == ^name)
  end

  def where_use_local_tunnel(query \\ base_query()) do
    from([http_endpoint: he] in query, where: he.use_local_tunnel == true)
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
    if endpoint.use_local_tunnel do
      %URI{
        scheme: "http",
        host: Application.fetch_env!(:sequin, :portal_hostname),
        port: endpoint.port,
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
