defmodule Sequin.Repo do
  use Ecto.Repo,
    otp_app: :sequin,
    adapter: Ecto.Adapters.Postgres
end
