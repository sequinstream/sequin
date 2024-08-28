defmodule SequinWeb.Layouts do
  @moduledoc """
  This module holds different layouts used by your application.

  See the `layouts` directory for all templates available.
  The "root" layout is a skeleton rendered as part of the
  application router. The "app" layout is set as the default
  layout on both `use SequinWeb, :controller` and
  `use SequinWeb, :live_view`.
  """
  use SequinWeb, :html

  embed_templates "layouts/*"

  attr :show_nav, :boolean, default: true
  attr :no_main, :boolean, default: false
  # Can be nil in test
  attr :current_path, :string, default: nil
  def app(assigns)

  def app_no_sidenav(assigns) do
    assigns = assign(assigns, :show_nav, false)

    app(assigns)
  end

  def app_no_main(assigns) do
    assigns = assign(assigns, :no_main, true)

    app(assigns)
  end

  def app_no_main_no_sidenav(assigns) do
    assigns = assign(assigns, :show_nav, false)
    assigns = assign(assigns, :no_main, true)

    app(assigns)
  end
end
