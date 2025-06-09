defmodule SequinWeb.Router do
  use SequinWeb, :router

  import Phoenix.LiveDashboard.Router
  import SequinWeb.UserAuth

  alias SequinWeb.Plugs.AssignCurrentPath
  alias SequinWeb.Plugs.VerifyApiToken
  alias SequinWeb.Plugs.VerifyContentType

  @self_hosted Application.compile_env!(:sequin, :self_hosted)

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:put_root_layout, html: {SequinWeb.Layouts, :root})
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
    plug(AssignCurrentPath)
    plug(:fetch_current_user)
  end

  pipeline :api do
    plug :accepts, ["json"]
    plug VerifyApiToken
    plug VerifyContentType
  end

  pipeline :admins_only do
    plug :admin_basic_auth
  end

  # if the user is not authenticated, we redirect them to the register page
  # Currently it's only used for the accept-invite flow
  pipeline :redirect_to_register_if_unauthenticated do
    plug :require_authenticated_user, unauthenticated_redirect: :register
  end

  scope "/", SequinWeb do
    pipe_through [:browser]

    get "/auth/github", UserSessionController, :start_oauth
    get "/auth/github/callback", UserSessionController, :callback
    delete "/logout", UserSessionController, :delete

    if @self_hosted or Application.compile_env(:sequin, :env) == :dev do
      get "/info/version", InfoController, :version
      get "/info", InfoController, :info
    end

    live_session :home do
      live "/", HomeLive, :index
    end

    live_session :current_user,
      on_mount: [{SequinWeb.UserAuth, :mount_current_user}],
      layout: {SequinWeb.Layouts, :app_no_sidenav} do
      live "/users/confirm/:token", UserConfirmationLive, :edit
      live "/users/confirm", UserConfirmationInstructionsLive, :new
    end
  end

  scope "/", SequinWeb do
    pipe_through [:browser, :redirect_if_user_is_authenticated]

    live_session :redirect_if_user_is_authenticated,
      on_mount: [{SequinWeb.UserAuth, :redirect_if_user_is_authenticated}, {SequinWeb.LiveHooks, :global}],
      layout: {SequinWeb.Layouts, :app_no_sidenav} do
      live "/register", UserRegistrationLive, :new
      live "/login", UserLoginLive, :new
      live "/users/reset_password", UserForgotPasswordLive, :new
      live "/users/reset_password/:token", UserResetPasswordLive, :edit

      if @self_hosted do
        live "/setup", SetupLive, :index
      end
    end

    post "/login", UserSessionController, :create
  end

  scope "/", SequinWeb do
    pipe_through [:browser, :redirect_to_register_if_unauthenticated]

    live_session :accept_invite, on_mount: [{SequinWeb.UserAuth, :ensure_authenticated}, {SequinWeb.LiveHooks, :global}] do
      live "/accept-invite/:token", AcceptInviteLive, :accept_invite
      live "/accept-team-invite/:token", AcceptInviteLive, :accept_team_invite
    end
  end

  scope "/", SequinWeb do
    pipe_through [:browser, :require_authenticated_user]

    live_session :require_authenticated_user,
      on_mount: [{SequinWeb.UserAuth, :ensure_authenticated}, {SequinWeb.LiveHooks, :global}] do
      live "/users/settings", UserSettingsLive, :edit
      live "/users/settings/confirm_email/:token", UserSettingsLive, :confirm_email
    end
  end

  scope "/", SequinWeb do
    pipe_through [:browser, :require_authenticated_user]

    live_session :default, on_mount: [{SequinWeb.UserAuth, :ensure_authenticated}, {SequinWeb.LiveHooks, :global}] do
      live "/sinks", SinkConsumersLive.Index, :index
      live "/sinks/new", SinkConsumersLive.Form, :new
      live "/sinks/:type/:id", SinkConsumersLive.Show, :show
      live "/sinks/:type/:id/backfills", SinkConsumersLive.Show, :backfills
      live "/sinks/:type/:id/messages", SinkConsumersLive.Show, :messages
      live "/sinks/:type/:id/messages/:ack_id", SinkConsumersLive.Show, :messages
      live "/sinks/:type/:id/trace", SinkConsumersLive.Show, :trace
      live "/sinks/:type/:id/edit", SinkConsumersLive.Form, :edit

      live "/functions", FunctionsLive.Index, :index
      live "/functions/new", FunctionsLive.Edit, :new
      live "/functions/:id", FunctionsLive.Edit, :edit

      live "/databases", DatabasesLive.Index, :index
      live "/databases/new", DatabasesLive.Form, :new
      live "/databases/:id", DatabasesLive.Show, :show
      live "/databases/:id/edit", DatabasesLive.Form, :edit
      live "/databases/:id/messages", DatabasesLive.Show, :messages

      live "/http-endpoints", HttpEndpointsLive.Index, :index
      live "/http-endpoints/new", HttpEndpointsLive.Form, :new
      live "/http-endpoints/:id", HttpEndpointsLive.Show, :show
      live "/http-endpoints/:id/edit", HttpEndpointsLive.Form, :edit

      live "/change-capture-pipelines", WalPipelinesLive.Index, :index
      live "/change-capture-pipelines/new", WalPipelinesLive.Form, :new
      live "/change-capture-pipelines/:id", WalPipelinesLive.Show, :show
      live "/change-capture-pipelines/:id/edit", WalPipelinesLive.Form, :edit

      live "/cli", CliLive, :index

      live "/logout", UserLogoutLive, :index

      get "/easter-egg", EasterEggController, :home

      live "/settings/accounts", Settings.AccountSettingsLive, :index
    end

    get "/admin/impersonate/:secret", UserSessionController, :impersonate
    get "/admin/unimpersonate", UserSessionController, :unimpersonate
  end

  scope "/health", SequinWeb do
    if @self_hosted do
      get "/", HealthCheckController, :check
    else
      get "/", HealthCheckController, :check_cloud
    end
  end

  scope "/api", SequinWeb do
    pipe_through(:api)

    # Postgres Database routes
    resources("/postgres_databases", PostgresDatabaseController, except: [:new, :edit], param: "id_or_name")
    resources("/api_keys", ApiKeyController, only: [:index, :create, :delete])

    resources("/postgres_replications", PostgresReplicationController, except: [:new, :edit])
    resources("/local_tunnels", LocalTunnelController, only: [:index])

    # HTTP Endpoints routes
    resources("/destinations/http_endpoints", HttpEndpointController, except: [:new, :edit], param: "id_or_name")

    # Sink Consumer routes
    resources("/sinks", SinkConsumerController, except: [:new, :edit], param: "id_or_name")
    # Backfill routes
    resources("/sinks/:sink_id_or_name/backfills", BackfillController, except: [:new, :edit, :delete])

    post("/postgres_databases/:id_or_name/test_connection", PostgresDatabaseController, :test_connection)
    post("/postgres_databases/:id_or_name/refresh_tables", PostgresDatabaseController, :refresh_tables)
    # get("/postgres_databases/:id_or_name/schemas", PostgresDatabaseController, :list_schemas)
    # get("/postgres_databases/:id_or_name/schemas/:schema/tables", PostgresDatabaseController, :list_tables)

    post("/sequin_streams/:id_or_name/receive", PullController, :receive)
    get("/sequin_streams/:id_or_name/receive", PullController, :receive)
    post("/sequin_streams/:id_or_name/ack", PullController, :ack)
    post("/sequin_streams/:id_or_name/nack", PullController, :nack)

    # For backwards compatibility
    post("/http_pull_consumers/:id_or_name/receive", PullController, :receive)
    get("/http_pull_consumers/:id_or_name/receive", PullController, :receive)
    post("/http_pull_consumers/:id_or_name/ack", PullController, :ack)
    post("/http_pull_consumers/:id_or_name/nack", PullController, :nack)

    post("/config/apply", YamlController, :apply)
    post("/config/plan", YamlController, :plan)
    get("/config/export", YamlController, :export)
  end

  scope "/" do
    pipe_through [:browser, :admins_only]

    live_dashboard("/admin/dashboard",
      metrics: SequinWeb.Telemetry,
      additional_pages: [
        broadway: BroadwayDashboard
      ]
    )
  end

  # Other scopes may use custom stacks.
  # scope "/api", SequinWeb do
  #   pipe_through :api
  # end

  # Enable Swoosh mailbox preview in development
  if Application.compile_env(:sequin, :dev_routes) do
    scope "/dev" do
      pipe_through(:browser)

      forward("/mailbox", Plug.Swoosh.MailboxPreview)
    end

    scope "/push-webhook" do
      post "/ack", SequinWeb.PushWebhookController, :ack
      post "/maybe-ack", SequinWeb.PushWebhookController, :maybe_ack
      post "/nack", SequinWeb.PushWebhookController, :nack
      post "/timeout", SequinWeb.PushWebhookController, :timeout
    end
  end

  defp admin_basic_auth(conn, _opts) do
    username = Application.get_env(:sequin, SequinWeb.Router)[:admin_user]
    password = Application.get_env(:sequin, SequinWeb.Router)[:admin_password]

    if is_binary(username) and is_binary(password) do
      Plug.BasicAuth.basic_auth(conn, username: username, password: password)
    else
      raise "Admin credentials not set. Please set ADMIN_USER and ADMIN_PASSWORD in your environment."
    end
  end
end
