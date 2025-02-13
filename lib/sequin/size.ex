defmodule Sequin.Size do
  @moduledoc """
  Helpers for working with sizes in bytes.

  Like `:timer.minutes/1` etc for bytes.
  """

  @spec bytes(bytes :: non_neg_integer()) :: bytes :: non_neg_integer()
  def bytes(n), do: n

  @spec kb(kilobytes :: non_neg_integer()) :: bytes :: non_neg_integer()
  def kb(n), do: n * 1024

  @spec mb(megabytes :: non_neg_integer()) :: bytes :: non_neg_integer()
  def mb(n), do: n * 1024 * 1024

  @spec gb(gigabytes :: non_neg_integer()) :: bytes :: non_neg_integer()
  def gb(n), do: n * 1024 * 1024 * 1024

  @spec tb(terabytes :: non_neg_integer()) :: bytes :: non_neg_integer()
  def tb(n), do: n * 1024 * 1024 * 1024 * 1024
end
