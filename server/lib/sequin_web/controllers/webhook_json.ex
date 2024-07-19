defmodule SequinWeb.WebhookJSON do
  @moduledoc false
  alias Sequin.Repo

  def render("index.json", %{webhooks: webhooks}) do
    %{data: Repo.preload(webhooks, :stream)}
  end

  def render("show.json", %{webhook: webhook}) do
    Repo.preload(webhook, :stream)
  end

  def render("delete.json", %{webhook: webhook}) do
    %{id: webhook.id, deleted: true}
  end
end
