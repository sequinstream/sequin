defmodule Sequin.Accounts.AllocatedBastionPort do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  alias __MODULE__

  schema "allocated_bastion_ports" do
    field :port, :integer
    field :name, :string
    belongs_to :account, Sequin.Accounts.Account

    timestamps()
  end

  def create_changeset(abp, attrs) do
    cast(abp, attrs, [:name])
  end

  def update_changeset(abp, attrs) do
    cast(abp, attrs, [:name])
  end

  def where_account_id(account_id) do
    from([allocated_bastion_port: abp] in base_query(), where: abp.account_id == ^account_id)
  end

  defp base_query(query \\ AllocatedBastionPort) do
    from(abp in query, as: :allocated_bastion_port)
  end
end
