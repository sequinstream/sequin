defmodule SequinWeb.StreamJSON do
  @moduledoc false
  alias Sequin.Streams.Stream

  def render("index.json", %{streams: streams}) do
    %{data: Enum.map(streams, &Stream.load_stats/1)}
  end

  def render("show.json", %{stream: stream}) do
    Stream.load_stats(stream)
  end

  def render("delete.json", %{stream: stream}) do
    %{id: stream.id, deleted: true}
  end
end
