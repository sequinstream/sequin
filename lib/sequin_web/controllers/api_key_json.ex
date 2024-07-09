defmodule SequinWeb.ApiKeyJSON do
  @moduledoc false

  def render("index.json", %{api_keys: api_keys}) do
    %{data: api_keys}
  end

  def render("show.json", %{api_key: api_key}) do
    api_key
  end
end
