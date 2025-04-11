# Example of using the persistent Starlark context for data transformations
# Run with: mix run examples/starlark_transform.exs

alias Sequin.Processors.Starlark

# Define transformation functions in Starlark
starlark_code = """
# Helper functions for transforming PostgreSQL data
def transform_user(user):
  # Add derived fields
  user["full_name"] = user["first_name"] + " " + user["last_name"]

  # Add a greeting based on the user's preferred language
  if user.get("language") == "es":
    user["greeting"] = "¡Hola, " + user["first_name"] + "!"
  elif user.get("language") == "fr":
    user["greeting"] = "Bonjour, " + user["first_name"] + "!"
  else:
    user["greeting"] = "Hello, " + user["first_name"] + "!"

  # Normalize email to lowercase
  if "email" in user:
    user["email"] = user["email"].lower()

  return user

# A counter to track how many records we've processed
state = {"processed_count": 0}

def get_stats():
  return state

def increment_counter():
  state["processed_count"] += 1
  return state["processed_count"]

# Process a batch of users
def process_users(users):
  results = []
  for user in users:
    results.append(transform_user(user))
    increment_counter()
  return results
"""

# Load Starlark code into the persistent context
IO.puts("Loading Starlark code...")
:ok = Starlark.load_code(starlark_code)

# Sample data that might come from a database
users = [
  %{
    "id" => 1,
    "first_name" => "Alice",
    "last_name" => "Smith",
    "email" => "Alice.Smith@EXAMPLE.com",
    "language" => "en"
  },
  %{
    "id" => 2,
    "first_name" => "Bob",
    "last_name" => "Jones",
    "email" => "bob.jones@example.com",
    "language" => "fr"
  },
  %{
    "id" => 3,
    "first_name" => "Carlos",
    "last_name" => "Rodriguez",
    "email" => "carlos@example.com",
    "language" => "es"
  }
]

# Process users one by one
IO.puts("\nProcessing users individually:")
Enum.each(users, fn user ->
  # Convert the Elixir map to JSON string
  user_json = Jason.encode!(user)

  # Call the Starlark function with the user data
  {:ok, transformed_user} = Starlark.call_function("transform_user", [user_json])

  # Print the transformed user
  IO.puts("Transformed: #{inspect(transformed_user)}")

  # Get the current count
  {:ok, count} = Starlark.call_function("increment_counter", [])
  IO.puts("Processed count: #{count}")
end)

# Process users as a batch
IO.puts("\nProcessing users as a batch:")
{:ok, transformed_users} = Starlark.call_function("process_users", [Jason.encode!(users)])
IO.puts("Batch result: #{inspect(transformed_users)}")

# Get stats
{:ok, stats} = Starlark.call_function("get_stats", [])
IO.puts("\nFinal stats: #{inspect(stats)}")
