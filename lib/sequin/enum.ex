defmodule Sequin.Enum do
  @moduledoc """
  Enum utilities. Functions defined in this file should feel like candidates for inclusion in an
  extended version of Elixir's Enum module.
  """
  def find!(enumberable, find_fun) do
    case Enum.find(enumberable, find_fun) do
      nil -> raise ArgumentError, "Could not find element"
      el -> el
    end
  end

  def take!(enumberable, count) do
    case Enum.take(enumberable, count) do
      list when length(list) < count -> raise ArgumentError, "Not enough elements in list to take"
      list -> list
    end
  end

  def find_index!(enumberable, find_fun) do
    case Enum.find_index(enumberable, find_fun) do
      nil -> raise ArgumentError, "Could not find element"
      index -> index
    end
  end
end
