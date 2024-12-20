defmodule Sequin.Finch do
  @moduledoc false
  def child_spec do
    {Finch, name: __MODULE__}
  end
end
