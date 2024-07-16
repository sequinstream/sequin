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
end
