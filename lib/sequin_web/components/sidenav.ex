defmodule SequinWeb.Components.Sidenav do
  # Change this line
  @moduledoc false
  use SequinWeb, :live_component

  import LiveSvelte

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Error

  @impl Phoenix.LiveComponent
  def handle_event("change_selected_account", %{"accountId" => account_id}, socket) do
    user = current_user(socket)
    account = Sequin.Enum.find!(user.accounts, &(&1.id == account_id))

    case Accounts.set_current_account_for_user(user.id, account.id) do
      {:ok, updated_user} ->
        {:noreply,
         socket
         |> assign(current_user: updated_user)
         |> push_navigate(to: socket.assigns.current_path)}

      {:error, _changeset} ->
        {:noreply, socket}
    end
  end

  @impl Phoenix.LiveComponent
  def handle_event("create_account", %{"accountName" => account_name}, socket) do
    user = current_user(socket)

    case create_and_associate_account(account_name, user) do
      {:ok, user} ->
        {:noreply,
         socket
         |> assign(current_user: user)
         |> push_navigate(to: socket.assigns.current_path)}

      {:error, %Ecto.Changeset{} = changeset} ->
        error = Error.validation(changeset: changeset)
        {:reply, %{error: Error.ValidationError.message(error)}, socket}

      {:error, _} ->
        {:reply, %{error: "Failed to create account"}, socket}
    end
  end

  defp create_and_associate_account(account_name, user) do
    with {:ok, account} <- Accounts.create_account(%{name: account_name}),
         {:ok, _account_user} <- Accounts.associate_user_with_account(user, account) do
      Accounts.set_current_account_for_user(user.id, account.id)
    else
      {:error, %Ecto.Changeset{} = changeset} -> {:error, changeset}
      error -> error
    end
  end

  attr :current_path, :string, required: true
  attr :current_user, :map, required: true

  def render(assigns) do
    assigns =
      assigns
      |> assign(:parent_id, "sidenav")
      |> assign(:current_account, User.current_account(assigns.current_user))

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="components/Sidenav"
        props={
          %{
            currentPath: @current_path,
            currentAccountId: @current_account.id,
            accountList: Enum.sort_by(@current_user.accounts, & &1.inserted_at, DateTime),
            currentUser: @current_user,
            parent: @parent_id
          }
        }
        socket={@socket}
      />
    </div>
    """
  end
end
