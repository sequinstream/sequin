defmodule SequinWeb.Router do
  use SequinWeb, :router

  import SequinWeb.UserAuth

  alias SequinWeb.Plugs.AssignCurrentPath
  alias SequinWeb.Plugs.VerifyApiToken

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
  end

  ## Authentication routes

  scope "/", SequinWeb do
    pipe_through [:browser]

    get "/auth/github", UserSessionController, :start_oauth
    get "/auth/github/callback", UserSessionController, :callback

    live_session :home do
      live "/", HomeLive, :index
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
    pipe_through [:browser, :require_authenticated_user]

    live_session :require_authenticated_user,
      on_mount: [{SequinWeb.UserAuth, :ensure_authenticated}] do
      live "/users/settings", UserSettingsLive, :edit
      live "/users/settings/confirm_email/:token", UserSettingsLive, :confirm_email
    end
  end

  scope "/", SequinWeb do
    pipe_through [:browser]

    delete "/logout", UserSessionController, :delete

    live_session :current_user,
      on_mount: [{SequinWeb.UserAuth, :mount_current_user}] do
      live "/users/confirm/:token", UserConfirmationLive, :edit
      live "/users/confirm", UserConfirmationInstructionsLive, :new
    end
  end

  scope "/", SequinWeb do
    pipe_through [:browser, :require_authenticated_user]

    live_session :default, on_mount: [{SequinWeb.UserAuth, :ensure_authenticated}, {SequinWeb.LiveHooks, :global}] do
      live "/consumers", ConsumersLive.Index, :index
      live "/consumers/new", ConsumersLive.Index, :new
      live "/consumers/:id", ConsumersLive.Show, :show
      live "/consumers/:id/edit", ConsumersLive.Show, :edit

      live "/databases", DatabasesLive.Index, :index
      live "/databases/new", DatabasesLive.Form, :new
      live "/databases/:id", DatabasesLive.Show, :show
      live "/databases/:id/edit", DatabasesLive.Form, :edit

      live "/http-endpoints", HttpEndpointsLive.Index, :index
      live "/http-endpoints/new", HttpEndpointsLive.Form, :new
      live "/http-endpoints/:id", HttpEndpointsLive.Show, :show
      live "/http-endpoints/:id/edit", HttpEndpointsLive.Form, :edit

      live "/logout", UserLogoutLive, :index

      get "/easter-egg", EasterEggController, :home

      live "/trace", TracerLive, :index
    end

    get "/admin/impersonate/:secret", UserSessionController, :impersonate
    get "/admin/unimpersonate", UserSessionController, :unimpersonate
  end

  scope "/health", SequinWeb do
    get "/", HealthCheckController, :check
  end

  scope "/api", SequinWeb do
    pipe_through(:api)

    resources("/streams/:stream_id_or_name/consumers", ConsumerController,
      except: [:new, :edit],
      param: "id_or_name"
    )

    resources("/databases", DatabaseController, except: [:new, :edit], param: "id_or_name")
    resources("/api_keys", ApiKeyController, only: [:index, :create, :delete])

    resources("/postgres_replications", PostgresReplicationController, except: [:new, :edit])

    post("/databases/:id_or_name/test_connection", DatabaseController, :test_connection)
    post("/databases/:id_or_name/setup_replication", DatabaseController, :setup_replication)
    get("/databases/:id_or_name/schemas", DatabaseController, :list_schemas)
    get("/databases/:id_or_name/schemas/:schema/tables", DatabaseController, :list_tables)
    post("/databases/test_connection", DatabaseController, :test_connection_params)
    post("/http_pull_consumers/:id_or_name/receive", PullController, :receive)
    get("/http_pull_consumers/:id_or_name/receive", PullController, :receive)
    post("/http_pull_consumers/:id_or_name/ack", PullController, :ack)
    post("/http_pull_consumers/:id_or_name/nack", PullController, :nack)
    post("/streams/:stream_id_or_name/messages", MessageController, :publish)
    get("/streams/:stream_id_or_name/messages", MessageController, :stream_list)
    get("/streams/:stream_id_or_name/messages/:key", MessageController, :stream_get)

    get(
      "/streams/:stream_id_or_name/messages/:key/consumer_info",
      MessageController,
      :message_consumer_info
    )

    get(
      "/streams/:stream_id_or_name/consumers/:consumer_id_or_name/messages",
      MessageController,
      :consumer_list
    )
  end

  # Other scopes may use custom stacks.
  # scope "/api", SequinWeb do
  #   pipe_through :api
  # end

  # Enable LiveDashboard and Swoosh mailbox preview in development
  if Application.compile_env(:sequin, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through(:browser)

      live_dashboard("/dashboard", metrics: SequinWeb.Telemetry)
      forward("/mailbox", Plug.Swoosh.MailboxPreview)
    end

    scope "/push-webhook" do
      post "/ack", SequinWeb.PushWebhookController, :ack
      post "/maybe-ack", SequinWeb.PushWebhookController, :maybe_ack
      post "/timeout", SequinWeb.PushWebhookController, :timeout
    end
  end
end
