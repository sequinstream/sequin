# Starlark Processor in Sequin

Sequin's Starlark processor provides a way to execute [Starlark](https://github.com/bazelbuild/starlark) code from Elixir applications. Starlark is a Python-like language designed for configuration and scripting.

## Usage

There are two main ways to use the Starlark processor:

1. **Single evaluation**: Execute Starlark code and get the result immediately
2. **Persistent context**: Load Starlark code once and call its functions repeatedly

## Single Evaluation

```elixir
# Simple evaluation
{:ok, result} = Sequin.Processors.Starlark.eval("1 + 1")
# result is 2

# Function definition and call
{:ok, result} = Sequin.Processors.Starlark.eval("""
def hello(name):
  return "Hello, " + name

hello("world")
""")
# result is "Hello, world"

# Working with data structures
{:ok, result} = Sequin.Processors.Starlark.eval("""
users = [
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25}
]
[u["name"] for u in users if u["age"] > 27]
""")
# result is ["Alice"]
```

## Module Imports

You can also evaluate Starlark code with module imports:

```elixir
# Define a module
math_module = """
def square(x):
  return x * x

def cube(x):
  return x * x * x
"""

# Use the module
{:ok, result} = Sequin.Processors.Starlark.eval_with_modules(
  """
  load('math.star', 'square', 'cube')
  [square(2), cube(3)]
  """,
  "main.star",
  %{"math.star" => math_module}
)
# result is [4, 27]
```

## Persistent Context API

The persistent context API allows you to load Starlark code once and then call its functions multiple times without re-parsing or re-evaluating the code. This is useful for performance-critical applications or when you want to maintain state between function calls.

### Loading Code

First, load your Starlark code:

```elixir
# Load Starlark code with function definitions
:ok = Sequin.Processors.Starlark.load_code("""
def add(a, b):
  return a + b

def multiply(a, b):
  return a * b

state = {"counter": 0}

def increment():
  state["counter"] += 1
  return state["counter"]
""")
```

### Calling Functions

After loading the code, you can call the defined functions:

```elixir
# Call functions from the loaded code
{:ok, result1} = Sequin.Processors.Starlark.call_function("add", [5, 7])
# result1 is 12

{:ok, result2} = Sequin.Processors.Starlark.call_function("multiply", [3, 4])
# result2 is 12

# The state is persistent between calls
{:ok, count1} = Sequin.Processors.Starlark.call_function("increment", [])
# count1 is 1

{:ok, count2} = Sequin.Processors.Starlark.call_function("increment", [])
# count2 is 2
```

## Built-in Functions

The Starlark processor comes with several built-in functions:

- `concat(a, b)`: Concatenates two strings
- `timestamp()`: Returns the current Unix timestamp
- `is_even(x)`: Returns whether a number is even

Example:

```elixir
{:ok, result} = Sequin.Processors.Starlark.eval("""
concat("Hello, ", "world")
""")
# result is "Hello, world"
```

## Error Handling

All functions return `{:ok, result}` on success and `{:error, reason}` on failure:

```elixir
case Sequin.Processors.Starlark.call_function("unknown_function", []) do
  {:ok, result} ->
    # Process the result
    IO.puts("Result: #{inspect(result)}")
  {:error, reason} ->
    # Handle the error
    IO.puts("Error: #{reason}")
end
```

## Considerations and Limitations

1. **Thread Safety**: The persistent context is thread-local. Each Erlang process will have its own Starlark context.
2. **Memory Management**: Code loaded with `load_code/1` will remain in memory until the process ends.
3. **Argument Types**: Only a limited set of data types are currently supported for function arguments: integers, floats, and strings.
4. **Security**: Be careful when executing user-provided Starlark code, as it can consume CPU and memory resources. 