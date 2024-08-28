defmodule SequinWeb.Components.TopBanner do
  @moduledoc false
  use Phoenix.Component

  import LiveSvelte

  alias Sequin.Accounts

  attr :current_account, :map, required: true

  def render(assigns) do
    account = assigns.current_account

    if account.is_temp do
      expires_at = Accounts.account_expires_at(account)
      almost_expired = DateTime.diff(expires_at, DateTime.utc_now(), :hour) <= 2
      assigns = assigns |> assign(expires_at: expires_at) |> assign(almost_expired: almost_expired)

      ~H"""
      <div id="top-banner">
        <.svelte
          name="components/TopBanner"
          props={%{expiresAt: @expires_at, almostExpired: @almost_expired}}
        />
      </div>
      """
    end
  end
end
