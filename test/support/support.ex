defmodule Sequin.TestSupport do
  @moduledoc false
  @spec expect_utc_now(non_neg_integer(), function()) :: Mox.t()
  def expect_utc_now(n \\ 1, fun) do
    Mox.expect(Sequin.TestSupport.DateTimeMock, :utc_now, n, fun)
  end
end
