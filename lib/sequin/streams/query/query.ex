defmodule Sequin.Streams.Query do
  @moduledoc false
  use Yesql, driver: Ecto, conn: Sequin.Repo

  # Important - include these declarations to force compilation when these files change
  @external_resource Path.join(__DIR__, "./next_for_consumer.sql")

  Yesql.defquery(Path.join(__DIR__, "./next_for_consumer.sql"))
end
