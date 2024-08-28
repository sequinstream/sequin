defmodule Sequin.Accounts.DeprovisionTempAccountsWorker do
  @moduledoc false
  use Oban.Worker, queue: :default

  alias Sequin.Accounts

  @impl Oban.Worker
  def perform(_job) do
    Enum.each(Accounts.list_expired_temp_accounts(), &Accounts.deprovision_account/1)
  end
end
