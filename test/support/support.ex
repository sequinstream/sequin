defmodule Sequin.TestSupport do
  @moduledoc false
  alias Sequin.TestSupport.DateTimeMock
  alias Sequin.TestSupport.EnumMock
  alias Sequin.TestSupport.ProcessMock
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

  @spec stub_random(function()) :: Mox.t()
  def stub_random(fun) do
    Mox.stub(EnumMock, :random, fun)
  end

  @spec stub_application_get_env(function()) :: Mox.t()
  def stub_application_get_env(fun) do
    Mox.stub(Sequin.ApplicationMock, :get_env, fun)
  end

  @spec expect_random(non_neg_integer(), function()) :: Mox.t()
  def expect_random(n \\ 1, fun) do
    Mox.expect(EnumMock, :random, n, fun)
  end

  @spec stub_process_alive?(function()) :: Mox.t()
  def stub_process_alive?(fun) do
    Mox.stub(ProcessMock, :alive?, fun)
  end

  @spec expect_process_alive?(non_neg_integer(), function()) :: Mox.t()
  def expect_process_alive?(n \\ 1, fun) do
    Mox.expect(ProcessMock, :alive?, n, fun)
  end

  @spec expect_application_get_env(non_neg_integer(), function()) :: Mox.t()
  def expect_application_get_env(n \\ 1, fun) do
    Mox.expect(Sequin.ApplicationMock, :get_env, n, fun)
  end
end
