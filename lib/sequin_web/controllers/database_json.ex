defmodule SequinWeb.DatabaseJSON do
  @moduledoc false

  def render("index.json", %{databases: databases}) do
    %{data: databases}
  end

  def render("show.json", %{database: database}) do
    database
  end

  def render("delete.json", %{database: database}) do
    %{id: database.id, deleted: true}
  end

  def render("test_connection.json", %{success: true}) do
    %{success: true}
  end

  def render("test_connection.json", %{success: false, reason: reason}) do
    %{success: false, reason: reason}
  end

  def render("setup_replication.json", %{slot_name: slot_name, publication_name: publication_name}) do
    %{
      success: true,
      slot_name: slot_name,
      publication_name: publication_name
    }
  end
end
