import Config

config :sequin, Sequin.Mailer,
  adapter: Sequin.Swoosh.Adapters.Loops,
  api_key: "TODO"

config :sequin, SequinWeb.UserSessionController,
  github: [
    redirect_uri: "TODO",
    client_id: "TODO",
    client_secret: "TODO"
  ]
