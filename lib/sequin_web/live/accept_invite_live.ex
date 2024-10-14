defmodule SequinWeb.AcceptInviteLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts
  alias Sequin.Accounts.User

  require Logger

  def mount(%{"token" => token}, _session, socket) do
    Logger.info("[AcceptInviteLive] Mounting with token: #{token}")

    user = current_user(socket)
    pid = self()

    {:ok,
     socket
     |> assign(:token, token)
     |> assign_async(:invite_result, fn ->
       accept_invite(user, token, pid)
     end), layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
  end

  def render(assigns) do
    ~H"""
    <div class="flex items-center justify-center min-h-screen mx-auto">
      <div class="w-full max-w-md">
        <.async_result :let={result} assign={@invite_result}>
          <:loading>
            <.status_card
              icon="hero-arrow-path"
              header="Accepting Invite"
              body="Please wait while we accept your invite..."
              icon_class="animate-spin text-black dark:text-white"
            />
          </:loading>
          <:failed :let={{:error, error}}>
            <.status_card
              icon="hero-exclamation-circle"
              header="Error"
              body={error}
              icon_class="text-red-500"
            />
          </:failed>
          <%= if result == :ok do %>
            <.status_card
              icon="hero-check-circle"
              header="Success"
              body="Your invite has been accepted successfully! Redirecting you shortly..."
              icon_class="text-green-500"
            />
          <% end %>
        </.async_result>
      </div>
    </div>
    """
  end

  defp status_card(assigns) do
    ~H"""
    <div class="p-6 rounded-lg shadow-lg min-w-96">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-2xl font-bold dark:text-white"><%= @header %></h2>
        <.icon name={@icon} class={@icon_class} />
      </div>
      <p class="text-gray-600 dark:text-gray-300"><%= @body %></p>
    </div>
    """
  end

  defp accept_invite(%User{} = user, token, pid) do
    Logger.info("[AcceptInviteLive] Accepting invite with token: #{token}")
    Process.sleep(1000)

    with {:ok, token} <- Base.url_decode64(token, padding: false),
         hashed_token = :crypto.hash(:sha256, token),
         {:ok, _} <- Accounts.accept_invite(user, hashed_token) do
      Logger.info("Invite accepted successfully, redirecting to home page")
      Process.send_after(pid, :redirect, 3000)

      {:ok, %{invite_result: :ok}}
    else
      :error ->
        Logger.error("Invalid base64 token, #{token}")
        {:error, "Invalid token. Please double check the invitation link and try again."}

      {:error, error} when is_exception(error) ->
        Logger.error("Error accepting invite: #{Exception.message(error)}")
        {:error, "Invalid token. Please double check the invitation link and try again."}
    end
  end

  def handle_info(:redirect, socket) do
    Logger.info("Redirecting to home page")
    {:noreply, redirect(socket, to: "/")}
  end
end
