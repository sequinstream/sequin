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

  scope "/api", SequinWeb do
    pipe_through :api

    resources "/streams", StreamController, except: [:new, :edit]
    resources "/streams/:stream_id/consumers", ConsumerController, except: [:new, :edit]
    resources "/databases", DatabaseController, except: [:new, :edit]
    resources "/api_keys", ApiKeyController, only: [:index, :create, :delete]
    resources "/postgres_replications", PostgresReplicationController, except: [:new, :edit]
    post "/databases/:id/test_connection", DatabaseController, :test_connection
    post "/databases/test_connection", DatabaseController, :test_connection_params
    post "/streams/:stream_id/consumers/:id/next", PullController, :next
    get "/streams/:stream_id/consumers/:id/next", PullController, :next
    post "/streams/:stream_id/consumers/:id/ack", PullController, :ack
    post "/streams/:stream_id/consumers/:id/nack", PullController, :nack
    post "/streams/:stream_id/messages", MessageController, :publish
    get "/streams/:stream_id/messages", MessageController, :stream_list
    get "/streams/:stream_id/consumers/:consumer_id/messages", MessageController, :consumer_list
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
