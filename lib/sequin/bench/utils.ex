defmodule Sequin.Bench.Utils do
  @moduledoc false
  def rand_string(size \\ 30) do
    for _ <- 1..size, into: "", do: <<Enum.random(~c"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")>>
  end
end
