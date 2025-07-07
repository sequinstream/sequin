defmodule SequinWeb.PostgresDatabaseJSON do
  @moduledoc false

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.String

  def render("index.json", %{databases: databases, show_sensitive: show_sensitive}) do
    %{data: Enum.map(databases, &render_db(&1, show_sensitive))}
  end

  def render("show.json", %{database: database, show_sensitive: show_sensitive}) do
    render_db(database, show_sensitive)
  end

  def render("delete.json", %{success: true, id: id}) do
    %{success: true, id: id}
  end

  def render("test_connection.json", %{success: true}) do
    %{success: true}
  end

  def render("test_connection.json", %{success: false, reason: reason}) do
    %{success: false, reason: reason}
  end

  def render("setup_replication.json", %{slot_name: slot_name, publication_name: publication_name, tables: tables}) do
    %{
      success: true,
      slot_name: slot_name,
      publication_name: publication_name,
      tables: tables
    }
  end

  def render("schemas.json", %{schemas: schemas}) do
    %{schemas: schemas}
  end

  def render("tables.json", %{tables: tables}) do
    %{tables: tables}
  end

  def render("refresh_tables.json", %{success: true}) do
    %{success: true}
  end

  def render("error.json", %{success: false}) do
    %{success: false}
  end

  def render("error.json", %{error: error}) do
    %{success: false, error: error}
  end

  defp render_db(%PostgresDatabase{replication_slot: slot, primary: primary} = database, show_sensitive) do
    r = %{
      id: database.id,
      name: database.name,
      hostname: database.hostname,
      port: database.port,
      database: database.database,
      username: database.username,
      password: render_password(database.password, show_sensitive),
      ssl: database.ssl,
      ipv6: database.ipv6,
      use_local_tunnel: database.use_local_tunnel,
      pool_size: database.pool_size,
      queue_interval: database.queue_interval,
      queue_target: database.queue_target,
      replication_slots: render_replication_slots(slot)
    }

    Sequin.Map.put_if_present(r, :primary, render_primary_database(primary))
  end

  defp render_password(password, true), do: password
  defp render_password(password, false), do: String.obfuscate(password)

  # Only nil in test atm
  defp render_replication_slots(nil), do: []

  defp render_replication_slots(%{publication_name: publication_name, slot_name: slot_name, status: status, id: id}) do
    [
      %{
        publication_name: publication_name,
        slot_name: slot_name,
        status: status,
        id: id
      }
    ]
  end

  defp render_primary_database(nil), do: nil

  defp render_primary_database(%{
         hostname: hostname,
         port: port,
         database: database_name,
         username: username,
         ssl: ssl,
         ipv6: ipv6
       }) do
    %{
      hostname: hostname,
      port: port,
      database: database_name,
      username: username,
      ssl: ssl,
      ipv6: ipv6
    }
  end
end
