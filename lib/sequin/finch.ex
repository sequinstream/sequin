defmodule Sequin.Finch do
  @moduledoc false
  def child_spec do
    {Finch, name: __MODULE__, pools: %{default: [size: 100, count: 10]}}
  end
end
