defmodule Sequin.TestSupport do
  @moduledoc false
  alias Sequin.TestSupport.DateTimeMock

  @spec expect_utc_now(non_neg_integer(), function()) :: Mox.t()
  def expect_utc_now(n \\ 1, fun) do
    Mox.expect(DateTimeMock, :utc_now, n, fun)
  end

  @spec stub_utc_now(function()) :: Mox.t()
  def stub_utc_now(fun) do
    Mox.stub(DateTimeMock, :utc_now, fun)
  end
end
