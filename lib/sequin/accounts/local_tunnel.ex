defmodule Sequin.Accounts.LocalTunnel do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Databases.PostgresDatabase

  @derive {Jason.Encoder, only: [:id, :account_id, :description, :bastion_port, :inserted_at, :updated_at]}

  @type id :: String.t()

  typed_schema "local_tunnels" do
    field :description, :string
    field :bastion_port, :integer, read_after_writes: true

    belongs_to :account, Account
    has_many :http_endpoints, HttpEndpoint
    has_many :postgres_databases, PostgresDatabase

    timestamps(type: :utc_datetime)
  end

  def create_changeset(local_tunnel, attrs) do
    local_tunnel
    |> cast(attrs, [:description, :bastion_port])
    |> validate_inclusion(:bastion_port, 10_000..65_535)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([local_tunnel: lt] in query, where: lt.account_id == ^account_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(lt in query, as: :local_tunnel)
  end
end
