defmodule SequinWeb.ConsumerJSON do
  @moduledoc false

  def render("index.json", %{consumers: consumers}) do
    %{data: consumers}
  end

  def render("show.json", %{consumer: consumer}) do
    consumer
  end

  def render("delete.json", %{consumer: consumer}) do
    %{id: consumer.id, deleted: true}
  end
end
