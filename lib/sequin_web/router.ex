defmodule SequinWeb.Router do
  use SequinWeb, :router

  alias SequinWeb.Plugs.AssignCurrentPath
  alias SequinWeb.Plugs.FetchUser

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:put_root_layout, html: {SequinWeb.Layouts, :root})
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
    plug(AssignCurrentPath)
  end

  pipeline :api do
    plug(:accepts, ["json"])
    plug(FetchUser)
  end

  scope "/", SequinWeb do
    pipe_through :browser

    live_session :setup, on_mount: [{SequinWeb.LiveHooks, :global}] do
      live "/", HomeLive, :index
    end

    live_session :default, on_mount: [SequinWeb.UserAuth, {SequinWeb.LiveHooks, :global}] do
      live "/consumers", ConsumersLive.Index, :index
      live "/consumers/new/quick", ConsumersLive.Index, :new
      live "/consumers/new", ConsumersLive.New, :new
      live "/consumers/:id", ConsumersLive.Show, :show
      live "/consumers/:id/edit", ConsumersLive.Show, :edit

      live "/databases", DatabasesLive.Index, :index
      live "/databases/new", DatabasesLive.Form, :new
      live "/databases/:id", DatabasesLive.Show, :show
      live "/databases/:id/edit", DatabasesLive.Form, :edit

      live "/http-endpoints", HttpEndpointsLive.Index, :index
      live "/http-endpoints/new", HttpEndpointsLive.New, :new
      live "/http-endpoints/:id", HttpEndpointsLive.Show, :show

      get "/easter-egg", EasterEggController, :home
    end
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
    post("/postgres_replications/:id/backfills", PostgresReplicationController, :create_backfills)

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
  end
end
