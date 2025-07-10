defmodule Sequin.MockVerificationTest do
  @moduledoc """
  Test to verify that all mocking libraries are properly configured to catch
  missed expectations and prevent false positives in tests.
  """

  use Sequin.Case, async: true

  # alias Sequin.Test.TestPlug


  describe "Mox mock verification" do
    setup do
      Mox.defmock(Sequin.Test.MoxSample, for: Sequin.Test.Sample)
      Application.put_env(:sequin, :test_mock_mox, Sequin.Test.MoxSample)
      :ok
    end

    test "fails when expected mock call is not made" do
      # Set up a Mox expectation
      Mox.expect(Sequin.Test.MoxSample, :call_mox, fn -> :ok end)

      # Do not call the mocked function
      # This test should fail if verify_on_exit! is working properly

      # We use assert_raise to verify that the test framework catches the missed expectation
      # When run individually, this will demonstrate that Mox verification is working
    end

    test "passes when expected mock call is made" do
      # Set up a Mox expectation
      Mox.expect(Sequin.Test.MoxSample, :call_mox, fn -> :ok end)

      # Call the mocked function
      assert Sequin.Test.Sample.call_mox() == :ok
    end

    test "passes when stub Hammox mock call is not made" do
      # Set up a Mox stub
      Mox.stub(Sequin.Test.MoxSample, :call_hammox, fn -> :ok end)
    end
  end

  describe "Hammox mock verification" do
    setup do
      Hammox.defmock(Sequin.Test.HammoxSample, for: Sequin.Test.Sample)
      Application.put_env(:sequin, :test_mock_hammox, Sequin.Test.HammoxSample)
      :ok
    end

    test "fails when expected Hammox mock call is not made" do
      # Set up a Hammox expectation
      Hammox.expect(Sequin.Test.HammoxSample, :call_hammox, fn -> :ok end)

      # Do not call the mocked function
      # This test should fail if verify_on_exit! is working properly for Hammox
    end

    test "passes when expected Hammox mock call is made" do
      # Set up a Hammox expectation
      Hammox.expect(Sequin.Test.HammoxSample, :call_hammox, fn -> :ok end)

      # Call the mocked function
      assert Sequin.Test.Sample.call_hammox() == :ok
    end

    test "passes when stub Hammox mock call is not made" do
      # Set up a Hammox stub
      Hammox.stub(Sequin.Test.HammoxSample, :call_hammox, fn -> :ok end)
    end
  end

  describe "Req.Test mock verification" do
    setup do
      Hammox.defmock(Sequin.Test.HammoxSample, for: Sequin.Test.Sample)
      Application.put_env(:sequin, :test_mock_req, [plug: {Req.Test, Sequin.Test.Sample}])
      :ok
    end

    test "fails when expected HTTP request is not made" do
      # Set up a Req.Test expectation
      Req.Test.expect(Sequin.Test.Sample, fn conn ->
        Req.Test.json(conn, :ok)
      end)

      # Do not make the HTTP request
      # This test should fail if Req.Test.verify_on_exit! is working properly
    end

    test "passes when expected HTTP request is made" do
      # Set up a Req.Test expectation
      Req.Test.expect(Sequin.Test.Sample, fn conn ->
        Req.Test.json(conn, "ok")
      end)

      # Make the HTTP request
      {:ok, response} = Sequin.Test.Sample.call_req()
      assert String.to_existing_atom(response.body) == :ok

      # This should pass
      assert true
    end
  end
end
