defmodule SequinWeb.StreamJSON do
  def render("show.json", %{stream: stream}) do
    %{stream: stream}
  end

  def render("index.json", %{streams: streams}) do
    %{streams: streams}
  end
end
