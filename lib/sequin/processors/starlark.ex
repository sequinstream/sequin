defmodule Sequin.Processors.Starlark do
  @moduledoc """
  Module for evaluating Starlark code using a NIF.

  This module provides functions to evaluate Starlark code with optional module imports,
  and to load and call Starlark functions with a persistent context.
  """
  use Rustler, otp_app: :sequin, crate: "sequin_processors_starlark"

  # When your NIF is loaded, it will override this function.
  def add(_a, _b), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Evaluates Starlark code and returns the result.

  ## Parameters
    * `code` - Binary containing Starlark code to evaluate

  ## Returns
    * `{:ok, result}` - The result of evaluation as a native Elixir term (decoded from JSON)
    * `{:error, reason}` - Error tuple with a reason when evaluation fails

  ## Examples
      iex> Sequin.Processors.Starlark.eval("1 + 1")
      {:ok, 2}

      iex> Sequin.Processors.Starlark.eval("def hello(): return 'world'; hello()")
      {:ok, "world"}

      iex> Sequin.Processors.Starlark.eval("[1, 2, 3]")
      {:ok, [1, 2, 3]}

      iex> Sequin.Processors.Starlark.eval("{'a': 1, 'b': 2}")
      {:ok, %{"a" => 1, "b" => 2}}

      iex> Sequin.Processors.Starlark.eval("concat('hello', ' world')")
      {:ok, "hello world"}
  """
  def eval(code) when is_binary(code) do
    json_result = eval_starlark(code)
    {:ok, Jason.decode!(json_result)}
  rescue
    e in Jason.DecodeError ->
      {:error, "Invalid JSON result: #{Exception.message(e)}"}

    e ->
      {:error, Exception.message(e)}
  end

  @doc """
  Evaluates Starlark code with module imports and returns the result.

  ## Parameters
    * `code` - Binary containing Starlark code to evaluate
    * `module_name` - Name of the main module (for error reporting)
    * `modules` - Map where keys are module names and values are module code (as binaries)

  ## Returns
    * `{:ok, result}` - The result of evaluation as a native Elixir term (decoded from JSON)
    * `{:error, reason}` - Error tuple with a reason when evaluation fails

  ## Examples
      iex> math_module = "def square(x): return x * x"
      iex> Sequin.Processors.Starlark.eval_with_modules(
      ...>   "load('math.star', 'square'); square(4)",
      ...>   "main.star",
      ...>   %{"math.star" => math_module}
      ...> )
      {:ok, 16}
  """
  def eval_with_modules(code, module_name, modules) when is_binary(code) and is_binary(module_name) and is_map(modules) do
    json_result = eval_starlark_with_modules(code, module_name, modules)
    {:ok, Jason.decode!(json_result)}
  rescue
    e in Jason.DecodeError ->
      {:error, "Invalid JSON result: #{Exception.message(e)}"}

    e ->
      {:error, Exception.message(e)}
  end

  @doc """
  Loads Starlark code into a persistent context.

  This function evaluates the code for its side effects, making functions defined in the code
  available for later calls with `call_function/2`.

  ## Parameters
    * `code` - Binary containing Starlark code to evaluate

  ## Returns
    * `:ok` - The code was successfully loaded
    * `{:error, reason}` - Error tuple with a reason when loading fails

  ## Examples
      iex> Sequin.Processors.Starlark.load_code("def add(a, b): return a + b")
      :ok

      iex> Sequin.Processors.Starlark.call_function("add", [1, 2])
      {:ok, 3}
  """
  def load_code(code) when is_binary(code) do
    load_code_nif(code)
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Calls a function previously defined in loaded Starlark code.

  ## Parameters
    * `function_name` - Name of the Starlark function to call
    * `args` - List of arguments to pass to the function

  ## Returns
    * `{:ok, result}` - The result of the function call as a native Elixir term (decoded from JSON)
    * `{:error, reason}` - Error tuple with a reason when the call fails

  ## Examples
      iex> Sequin.Processors.Starlark.load_code("def greet(name): return 'Hello, ' + name")
      :ok

      iex> Sequin.Processors.Starlark.call_function("greet", ["World"])
      {:ok, "Hello, World"}
  """
  def call_function(function_name, args) when is_binary(function_name) and is_list(args) do
    dbg(function_name)
    json_result = call_function_nif(function_name, args)

    case Jason.decode(json_result) do
      {:ok, result} ->
        {:ok, result}

      # If it's an error, it won't necessarily be JSON
      {:error, reason} ->
        {:error, reason}
    end
  rescue
    e in Jason.DecodeError ->
      {:error, "Invalid JSON result: #{Exception.message(e)}"}

    e ->
      {:error, Exception.message(e)}
  end

  @doc """
  Evaluates Starlark code and returns the raw result as JSON.

  ## Parameters
    * `code` - Binary containing Starlark code to evaluate

  ## Returns
    * `binary` - JSON string representing the result
  """
  def eval_starlark(_code), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Evaluates Starlark code with module imports and returns the raw result as JSON.

  ## Parameters
    * `code` - Binary containing Starlark code to evaluate
    * `module_name` - Name of the main module (for error reporting)
    * `modules` - Map where keys are module names and values are module code (as binaries)

  ## Returns
    * `binary` - JSON string representing the result
  """
  def eval_starlark_with_modules(_code, _module_name, _modules), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Raw NIF function for loading Starlark code into the persistent context.
  """
  def load_code_nif(_code), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Raw NIF function for calling a function in the Starlark context.
  """
  def call_function_nif(_function_name, _args), do: :erlang.nif_error(:nif_not_loaded)
end
