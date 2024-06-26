defmodule SequinStream.Repo do
  use Ecto.Repo,
    otp_app: :sequin_stream,
    adapter: Ecto.Adapters.Postgres
end
