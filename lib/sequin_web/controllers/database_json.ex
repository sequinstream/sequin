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
end
