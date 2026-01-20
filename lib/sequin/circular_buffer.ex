# From https://github.com/elixir-toniq/circular_buffer/blob/main/lib/circular_buffer.ex

# SPDX-FileCopyrightText: 2019 Chris Keathley
# SPDX-FileCopyrightText: 2020 Frank Hunleth
# SPDX-FileCopyrightText: 2022 Milton Mazzarri
#
# SPDX-License-Identifier: MIT
#
defmodule Sequin.CircularBuffer do
  @moduledoc """
  Circular Buffer

  When creating a circular buffer you must specify the max size:

  ```
  cb = CircularBuffer.new(10)
  ```

  CircularBuffers are implemented as Okasaki queues like Erlang's `:queue`
  module, but with additional optimizations thanks to the reduced set
  of operations.

  CircularBuffer implements both the
  [`Enumerable`](https://hexdocs.pm/elixir/Enumerable.html) and
  [`Collectable`](https://hexdocs.pm/elixir/Collectable.html) protocols, so code
  like the following works:

      iex> cb = Enum.into([1, 2, 3, 4], CircularBuffer.new(3))
      #CircularBuffer<[2, 3, 4]>
      iex> Enum.map(cb, fn x -> x * 2 end)
      [4, 6, 8]
  """

  alias __MODULE__, as: CB

  defstruct [:a, :b, :max_size, :count]
  @typedoc "A circular buffer"
  @opaque t() :: %__MODULE__{
            a: list(),
            b: list(),
            max_size: pos_integer(),
            count: non_neg_integer()
          }

  @doc """
  Creates a new circular buffer with a given size.
  """
  @spec new(pos_integer()) :: t()
  def new(size) when is_integer(size) and size > 0 do
    %CB{a: [], b: [], max_size: size, count: 0}
  end

  @doc """
  Inserts a new item into the next location of the circular buffer
  """
  @spec insert(t(), any()) :: t()
  def insert(%CB{b: b} = cb, item) when b != [] do
    %{cb | a: [item | cb.a], b: tl(b)}
  end

  def insert(%CB{count: count, max_size: max_size} = cb, item) when count < max_size do
    %{cb | a: [item | cb.a], count: cb.count + 1}
  end

  def insert(%CB{b: []} = cb, item) do
    new_b = cb.a |> Enum.reverse() |> tl()
    %{cb | a: [item], b: new_b}
  end

  @doc """
  Converts a circular buffer to a list. The list is ordered from oldest to newest
  elements based on their insertion order.
  """
  @spec to_list(t()) :: list()
  def to_list(%CB{} = cb) do
    cb.b ++ Enum.reverse(cb.a)
  end

  @doc """
  Returns the newest element in the buffer

  ## Examples

      iex> cb = CircularBuffer.new(3)
      iex> CircularBuffer.newest(cb)
      nil
      iex> cb = Enum.reduce(1..4, cb, fn n, cb -> CircularBuffer.insert(cb, n) end)
      iex> CircularBuffer.newest(cb)
      4

  """
  @spec newest(t()) :: any()
  def newest(%CB{a: [newest | _rest]}), do: newest
  def newest(%CB{b: []}), do: nil

  @doc """
  Returns the oldest element in the buffer
  """
  @spec oldest(t()) :: any()
  def oldest(%CB{b: [oldest | _rest]}), do: oldest
  def oldest(%CB{a: a}), do: List.last(a)

  @doc """
  Checks the buffer to see if its empty

  Returns `true` if the given circular buffer is empty, otherwise `false`.

  ## Examples

      iex> cb = CircularBuffer.new(1)
      iex> CircularBuffer.empty?(cb)
      true
      iex> cb |> CircularBuffer.insert(1) |> CircularBuffer.empty?()
      false

  """
  @spec empty?(t()) :: boolean()
  def empty?(%CB{} = cb) do
    cb.count == 0
  end

  defimpl Enumerable do
    def count(cb) do
      {:ok, cb.count}
    end

    def member?(cb, element) do
      {:ok, Enum.member?(cb.a, element) or Enum.member?(cb.b, element)}
    end

    def reduce(cb, acc, fun) do
      Enumerable.List.reduce(CB.to_list(cb), acc, fun)
    end

    def slice(_cb) do
      {:error, __MODULE__}
    end
  end

  defimpl Collectable do
    def into(original) do
      collector_fn = fn
        cb, {:cont, elem} -> CB.insert(cb, elem)
        cb, :done -> cb
        _cb, :halt -> :ok
      end

      {original, collector_fn}
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(cb, opts) do
      concat(["#CircularBuffer<", to_doc(CB.to_list(cb), opts), ">"])
    end
  end
end
