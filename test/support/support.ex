defmodule Sequin.TestSupport do
  @moduledoc false
  alias Sequin.TestSupport.DateTimeMock
  alias Sequin.TestSupport.UUIDMock

  @spec expect_utc_now(non_neg_integer(), function()) :: Mox.t()
  def expect_utc_now(n \\ 1, fun) do
    Mox.expect(DateTimeMock, :utc_now, n, fun)
  end

  @spec expect_uuid4(non_neg_integer(), function()) :: Mox.t()
  def expect_uuid4(n \\ 1, fun) do
    Mox.expect(UUIDMock, :uuid4, n, fun)
  end

  @spec stub_utc_now(function()) :: Mox.t()
  def stub_utc_now(fun) do
    Mox.stub(DateTimeMock, :utc_now, fun)
  end

  @spec stub_uuid4(function()) :: Mox.t()
  def stub_uuid4(fun) do
    Mox.stub(UUIDMock, :uuid4, fun)
  end
end
