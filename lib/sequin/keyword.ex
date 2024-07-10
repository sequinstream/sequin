defmodule Sequin.Keyword do
  @moduledoc """
  Keyword utilities. Functions defined in this file should feel like candidates for inclusion in an
  extended version of Elixir's Keyword module.
  """

  def put_if_present(keyword, key, value) do
    if is_nil(value) do
      keyword
    else
      Keyword.put(keyword, key, value)
    end
  end

  def reject_nils(keyword) do
    Enum.reject(keyword, fn {_, v} -> is_nil(v) end)
  end
end
