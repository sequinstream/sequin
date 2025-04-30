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

  defp render_db(%PostgresDatabase{replication_slot: slot} = database, show_sensitive) do
    password = if show_sensitive, do: database.password, else: String.obfuscate(database.password)

    # Only nil in test atm
    replication_slots_data =
      if is_nil(slot) do
        []
      else
        [
          %{
            publication_name: slot.publication_name,
            slot_name: slot.slot_name,
            status: slot.status,
            id: slot.id
          }
        ]
      end

    %{
      id: database.id,
      name: database.name,
      hostname: database.hostname,
      port: database.port,
      database: database.database,
      username: database.username,
      password: password,
      ssl: database.ssl,
      ipv6: database.ipv6,
      use_local_tunnel: database.use_local_tunnel,
      pool_size: database.pool_size,
      queue_interval: database.queue_interval,
      queue_target: database.queue_target,
      replication_slots: replication_slots_data
    }
  end
end
