defmodule Sequin.Consumers.Query do
  @moduledoc false
  use Yesql, driver: Ecto, conn: Sequin.Repo

  # Important - include these declarations to force compilation when these files change
  @external_resource Path.join(__DIR__, "upsert_consumer_records.sql")
  @external_resource Path.join(__DIR__, "receive_consumer_events.sql")

  Yesql.defquery(Path.join(__DIR__, "upsert_consumer_records.sql"))
  Yesql.defquery(Path.join(__DIR__, "receive_consumer_events.sql"))
end
