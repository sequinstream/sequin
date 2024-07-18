defmodule SequinWeb.Router do
  use SequinWeb, :router

  alias SequinWeb.Plugs.FetchUser

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {SequinWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
    plug FetchUser
  end

  scope "/", SequinWeb do
    pipe_through :browser

    get "/", PageController, :home
  end

  scope "/api", SequinWeb do
    pipe_through :api

    resources "/streams", StreamController, except: [:new, :edit], param: "id_or_name"
    resources "/streams/:stream_id_or_name/consumers", ConsumerController, except: [:new, :edit], param: "id_or_name"
    resources "/databases", DatabaseController, except: [:new, :edit]
    resources "/api_keys", ApiKeyController, only: [:index, :create, :delete]

    resources "/postgres_replications", PostgresReplicationController, except: [:new, :edit]
    post "/postgres_replications/:id/backfills", PostgresReplicationController, :create_backfills

    post "/databases/:id/test_connection", DatabaseController, :test_connection
    post "/databases/:id/setup_replication", DatabaseController, :setup_replication
    get "/databases/:id/schemas", DatabaseController, :list_schemas
    get "/databases/:id/schemas/:schema/tables", DatabaseController, :list_tables
    post "/databases/test_connection", DatabaseController, :test_connection_params
    post "/streams/:stream_id_or_name/consumers/:id_or_name/receive", PullController, :receive
    get "/streams/:stream_id_or_name/consumers/:id_or_name/receive", PullController, :receive
    post "/streams/:stream_id_or_name/consumers/:id_or_name/ack", PullController, :ack
    post "/streams/:stream_id_or_name/consumers/:id_or_name/nack", PullController, :nack
    post "/streams/:stream_id_or_name/messages", MessageController, :publish
    get "/streams/:stream_id_or_name/messages", MessageController, :stream_list
    get "/streams/:stream_id_or_name/messages/:key", MessageController, :stream_get
    get "/streams/:stream_id_or_name/consumers/:consumer_id_or_name/messages", MessageController, :consumer_list
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
      pipe_through :browser

      live_dashboard "/dashboard", metrics: SequinWeb.Telemetry
      forward "/mailbox", Plug.Swoosh.MailboxPreview
    end
  end
end
