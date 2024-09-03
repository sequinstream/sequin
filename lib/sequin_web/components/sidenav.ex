defmodule SequinWeb.Components.Sidenav do
  @moduledoc false
  use Phoenix.Component

  import LiveSvelte

  attr :current_path, :string, required: true
  attr :account_name, :string, required: true

  def render(assigns) do
    ~H"""
    <.svelte
      name="components/Sidenav"
      props={%{currentPath: @current_path, accountName: @account_name}}
    />
    """
  end
end
