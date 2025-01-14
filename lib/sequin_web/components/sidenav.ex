defmodule SequinWeb.Components.Sidenav do
  @moduledoc false
  use SequinWeb, :live_component

  import LiveSvelte

  alias Sequin.Accounts
  alias Sequin.Consumers
  alias Sequin.Error

  @impl Phoenix.LiveComponent
  def update(assigns, socket) do
    socket = assign(socket, assigns)
    account = current_account(socket)
    earliest_sink_inserted_at = Consumers.earliest_sink_consumer_inserted_at_for_account(account.id)

    socket =
      socket
      |> assign(
        current_account: current_account(socket),
        accounts: accounts(socket),
        earliest_sink_inserted_at: earliest_sink_inserted_at,
        release_version: Application.get_env(:sequin, :release_version)
      )
      |> assign_async(:latest_version, fn ->
        case Req.get("https://sequinstream.com/gh/releases/latest/sequinstream/sequin") do
          {:ok, %{body: %{"tag_name" => tag_name}}} -> {:ok, %{latest_version: tag_name}}
          _ -> {:ok, %{latest_version: nil}}
        end
      end)

    {:ok, socket}
  end

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
        {:reply, %{error: Exception.message(error)}, socket}

      {:error, _} ->
        {:reply, %{error: "Failed to create account"}, socket}
    end
  end

  defp create_and_associate_account(account_name, user) do
    with {:ok, account} <- Accounts.create_account(%{name: account_name}),
         {:ok, _account_user} <- Accounts.associate_user_with_account(user, account) do
      Accounts.set_current_account_for_user(user.id, account.id)
    end
  end

  attr :current_path, :string, required: true
  attr :current_user, :map, required: true

  def render(assigns) do
    latest_version =
      case assigns.latest_version do
        %{ok?: true, result: version} when not is_nil(version) and version != assigns.release_version ->
          version

        _ ->
          nil
      end

    sink_inserted_over_5_min_ago? =
      not is_nil(assigns.earliest_sink_inserted_at) and
        DateTime.diff(DateTime.utc_now(), assigns.earliest_sink_inserted_at, :minute) > 5

    assigns =
      assigns
      |> assign(:parent_id, "sidenav")
      |> assign(
        :settings_has_notifications,
        sink_inserted_over_5_min_ago? and Accounts.Account.show_contact_email_alert?(assigns.current_account)
      )
      |> assign(:latest_version, latest_version)

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="components/Sidenav"
        props={
          %{
            currentPath: @current_path,
            currentAccountId: @current_account.id,
            accountList: Enum.sort_by(@accounts, & &1.inserted_at, DateTime),
            currentUser: @current_user,
            parent: @parent_id,
            accountSettingsHasNotification: @settings_has_notifications,
            sequinVersion: @release_version,
            latestVersion: @latest_version
          }
        }
        socket={@socket}
        class="h-screen sticky top-0 z-[49]"
      />
    </div>
    """
  end
end
