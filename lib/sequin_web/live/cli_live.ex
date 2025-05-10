defmodule SequinWeb.CliLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts
  alias Sequin.ApiTokens

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    socket =
      assign(socket, :api_base_url, Application.fetch_env!(:sequin, :api_base_url))

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    current_account = Accounts.User.current_account(assigns.current_user)
    api_tokens = ApiTokens.list_tokens_for_account(current_account.id)
    encoded_api_tokens = encode_api_tokens(api_tokens)

    assigns =
      assign(assigns, :api_tokens, encoded_api_tokens)

    ~H"""
    <div id="cli-page">
      <.svelte
        name="cli/Index"
        props={
          %{
            apiTokens: @api_tokens,
            apiBaseUrl: @api_base_url
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  defp encode_api_tokens(tokens) do
    Enum.map(tokens, fn token ->
      %{
        id: token.id,
        name: token.name,
        token: token.token,
        insertedAt: token.inserted_at
      }
    end)
  end
end
