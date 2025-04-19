defmodule Sequin.Processors.StarlarkTest do
  use ExUnit.Case, async: false

  alias Sequin.Processors.Starlark

  describe "traditional evaluation" do
    test "eval/1 evaluates Starlark code" do
      assert {:ok, 2} = Starlark.eval("1 + 1")
    end

    test "eval/1 can define and call functions" do
      code = """
      def hello(name):
        return "Hello, " + name

      hello("world")
      """

      assert {:ok, "Hello, world"} = Starlark.eval(code)
    end

    test "eval_with_modules/3 can import modules" do
      math_module = """
      def square(x):
        return x * x
      """

      code = """
      load('math.star', 'square')
      square(4)
      """

      assert {:ok, 16} = Starlark.eval_with_modules(code, "main.star", %{"math.star" => math_module})
    end

    test "built-in functions work" do
      assert {:ok, "hello world"} = Starlark.eval("concat('hello', ' world')")
      assert {:ok, true} = Starlark.eval("is_even(2)")
      assert {:ok, false} = Starlark.eval("is_even(3)")

      # Timestamp should return a number
      {:ok, timestamp} = Starlark.eval("timestamp()")
      assert is_integer(timestamp)
    end
  end

  describe "persistent context" do
    test "load_code/1 loads code and call_function/2 calls functions" do
      code = """
      def add(a, b):
        return a + b

      def subtract(a, b):
        return a - b
      """

      assert :ok = Starlark.load_code(code)
      assert {:ok, 5} = Starlark.call_function("add", [2, 3])
      assert {:ok, -1} = Starlark.call_function("subtract", [2, 3])
    end

    test "state is maintained between function calls" do
      code = """
      state = {"counter": 0}

      def increment():
        state["counter"] += 1
        return state["counter"]
      """

      assert :ok = Starlark.load_code(code)
      assert {:ok, 1} = Starlark.call_function("increment", [])
      assert {:ok, 2} = Starlark.call_function("increment", [])
      assert {:ok, 3} = Starlark.call_function("increment", [])
    end

    test "different data types as arguments" do
      code = """
      def echo(x):
        return x

      def type_of(x):
        return type(x).__name__
      """

      assert :ok = Starlark.load_code(code)
      assert {:ok, 42} = Starlark.call_function("echo", [42])
      assert {:ok, 3.14} = Starlark.call_function("echo", [3.14])
      assert {:ok, "hello"} = Starlark.call_function("echo", ["hello"])

      assert {:ok, "int"} = Starlark.call_function("type_of", [42])
      assert {:ok, "float"} = Starlark.call_function("type_of", [3.14])
      assert {:ok, "string"} = Starlark.call_function("type_of", ["hello"])
    end

    test "error handling for unknown functions" do
      assert {:error, _} = Starlark.call_function("unknown_function", [])
    end
  end
end
