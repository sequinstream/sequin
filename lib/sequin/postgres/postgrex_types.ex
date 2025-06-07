Postgrex.Types.define(Sequin.Postgres.PostgrexTypes, Pgvector.extensions() ++ Ecto.Adapters.Postgres.extensions(),
  allow_infinite_timestamps: true
)
