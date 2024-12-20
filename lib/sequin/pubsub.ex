defmodule Sequin.PubSub do
  @moduledoc false
  def child_spec do
    {Phoenix.PubSub, name: __MODULE__}
  end
end
