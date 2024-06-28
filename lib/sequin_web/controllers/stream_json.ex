defmodule SequinWeb.StreamJSON do
  @moduledoc false

  def render("index.json", %{streams: streams}) do
    %{data: streams}
  end

  def render("show.json", %{stream: stream}) do
    stream
  end

  def render("delete.json", %{stream: stream}) do
    %{id: stream.id, deleted: true}
  end
end
