defmodule Sequin.List do
  @moduledoc """
  List utilities. Functions defined in this file should feel like candidates for inclusion in an
  extended version of Elixir's List module.
  """

  @spec maybe_append(List.t(), boolean(), List.t()) :: List.t()
  def maybe_append(list1, bool, list2) do
    if bool do
      list1 ++ list2
    else
      list1
    end
  end
end
