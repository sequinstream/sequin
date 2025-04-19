defmodule Sequin.Processors.StarlarkPersistentTest do
  use ExUnit.Case, async: false

  alias Sequin.Processors.Starlark

  setup do
    # Load a test script with stateful functions into the persistent context
    starlark_code = """
    # Maintain counters in state
    state = {"counter": 0, "calls": {}}

    def increment(amount = 1):
      state["counter"] += amount
      return state["counter"]

    def get_counter():
      return state["counter"]

    def track_call(function_name):
      if function_name in state["calls"]:
        state["calls"][function_name] += 1
      else:
        state["calls"][function_name] = 1
      return state["calls"][function_name]

    def get_call_stats():
      return state["calls"]

    # Process JSON data
    def process_record(record):
      # Track this call
      track_call("process_record")

      # Transform the record
      record["processed"] = True
      record["timestamp"] = timestamp()
      return record
    """

    :ok = Starlark.load_code(starlark_code)
    :ok
  end

  test "counter maintains state between calls" do
    # Initial value
    assert {:ok, 0} = Starlark.call_function("get_counter", [])

    # Increment by default amount (1)
    assert {:ok, 1} = Starlark.call_function("increment", [])

    # Check new value
    assert {:ok, 1} = Starlark.call_function("get_counter", [])

    # Increment by specified amount
    assert {:ok, 6} = Starlark.call_function("increment", [5])

    # Check new value
    assert {:ok, 6} = Starlark.call_function("get_counter", [])
  end

  test "call tracking maintains a record of function calls" do
    # First call to process_record
    record = %{"id" => 123, "name" => "Test Record"}
    {:ok, _} = Starlark.call_function("process_record", [Jason.encode!(record)])

    # Check call count
    {:ok, call_stats} = Starlark.call_function("get_call_stats", [])
    assert call_stats["process_record"] == 1

    # Call process_record again
    record2 = %{"id" => 456, "name" => "Another Record"}
    {:ok, _} = Starlark.call_function("process_record", [Jason.encode!(record2)])

    # Check updated call count
    {:ok, call_stats} = Starlark.call_function("get_call_stats", [])
    assert call_stats["process_record"] == 2
  end

  test "built-in functions are accessible in persistent context" do
    # Define a test record
    record = %{"id" => 789, "name" => "Timestamp Test"}

    # Process the record which uses the timestamp() built-in function
    {:ok, processed} = Starlark.call_function("process_record", [Jason.encode!(record)])

    # Verify the timestamp was added
    assert processed["processed"] == true
    assert is_integer(processed["timestamp"])
  end

  test "complex data structures in persistent state" do
    # Define a complex tracking script
    complex_code = """
    tracking = {
      "users": {},
      "events": []
    }

    def track_user(user_id, action):
      if user_id not in tracking["users"]:
        tracking["users"][user_id] = {"actions": []}

      tracking["users"][user_id]["actions"].append(action)
      tracking["events"].append({"user_id": user_id, "action": action, "time": timestamp()})

      return tracking["users"][user_id]

    def get_tracking_data():
      return tracking
    """

    # Load the complex tracking code
    :ok = Starlark.load_code(complex_code)

    # Track some user actions
    {:ok, _} = Starlark.call_function("track_user", ["user123", "login"])
    {:ok, _} = Starlark.call_function("track_user", ["user456", "view_page"])
    {:ok, _} = Starlark.call_function("track_user", ["user123", "click_button"])

    # Get the tracking data
    {:ok, tracking_data} = Starlark.call_function("get_tracking_data", [])

    # Verify the data structure
    assert map_size(tracking_data["users"]) == 2
    assert length(tracking_data["events"]) == 3
    assert length(tracking_data["users"]["user123"]["actions"]) == 2
  end
end
