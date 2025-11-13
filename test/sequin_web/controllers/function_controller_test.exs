defmodule SequinWeb.FunctionControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.FunctionsFactory

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    function = FunctionsFactory.insert_function!(account_id: account.id)

    other_function =
      FunctionsFactory.insert_function!(account_id: other_account.id)

    %{function: function, other_function: other_function, other_account: other_account}
  end

  describe "index" do
    test "lists functions in the given account", %{
      conn: conn,
      account: account,
      function: function
    } do
      another_function = FunctionsFactory.insert_function!(account_id: account.id)

      conn = get(conn, ~p"/api/functions")
      assert %{"data" => functions} = json_response(conn, 200)
      assert length(functions) == 2
      atomized_functions = Enum.map(functions, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([function, another_function], atomized_functions, &(&1.id == &2.id))
    end

    test "does not list functions from another account", %{
      conn: conn,
      other_function: other_function
    } do
      conn = get(conn, ~p"/api/functions")
      assert %{"data" => functions} = json_response(conn, 200)
      refute Enum.any?(functions, &(&1["id"] == other_function.id))
    end
  end

  describe "show" do
    test "shows function details by id", %{conn: conn, function: function} do
      conn = get(conn, ~p"/api/functions/#{function.id}")
      assert function_json = json_response(conn, 200)

      assert function.name == function_json["name"]
      assert function.id == function_json["id"]
    end

    test "shows function details by name", %{conn: conn, function: function} do
      conn = get(conn, ~p"/api/functions/#{function.name}")
      assert function_json = json_response(conn, 200)

      assert function.name == function_json["name"]
      assert function.id == function_json["id"]
    end

    test "returns 404 if function belongs to another account", %{
      conn: conn,
      other_function: other_function
    } do
      conn = get(conn, ~p"/api/functions/#{other_function.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a filter function under the authenticated account", %{
      conn: conn,
      account: account
    } do
      function_attrs = %{
        name: "my-filter",
        description: "Filter records with value > 40",
        type: "filter",
        code: """
        def filter(action, record, changes, metadata) do
          record["value"] > 40
        end
        """
      }

      conn = post(conn, ~p"/api/functions", function_attrs)
      assert %{"name" => name, "id" => id} = json_response(conn, 200)

      {:ok, function} = Consumers.get_function_for_account(account.id, id)
      assert function.account_id == account.id
      assert function.name == name
      assert function.type == "filter"
    end

    test "creates a transform function with nested structure", %{
      conn: conn,
      account: account
    } do
      function_attrs = %{
        name: "my-transform",
        description: "Extract ID and action",
        function: %{
          type: "transform",
          code: """
          def transform(action, record, changes, metadata) do
            %{id: record["id"], action: action}
          end
          """
        }
      }

      conn = post(conn, ~p"/api/functions", function_attrs)
      assert %{"name" => name, "id" => id} = json_response(conn, 200)

      {:ok, function} = Consumers.get_function_for_account(account.id, id)
      assert function.account_id == account.id
      assert function.name == name
      assert function.type == "transform"
    end

    test "creates a path function", %{
      conn: conn,
      account: account
    } do
      function_attrs = %{
        name: "my-path",
        description: "Extract record",
        type: "path",
        path: "record"
      }

      conn = post(conn, ~p"/api/functions", function_attrs)
      assert %{"name" => name, "id" => id} = json_response(conn, 200)

      {:ok, function} = Consumers.get_function_for_account(account.id, id)
      assert function.account_id == account.id
      assert function.name == name
      assert function.type == "path"
    end

    test "creates a routing function", %{
      conn: conn,
      account: account
    } do
      function_attrs = %{
        name: "my-routing",
        description: "Route to REST API",
        type: "routing",
        sink_type: "http_push",
        code: """
        def route(action, record, changes, metadata) do
          %{
            method: "POST",
            endpoint_path: "/api/users/\#{record["id"]}"
          }
        end
        """
      }

      conn = post(conn, ~p"/api/functions", function_attrs)
      assert %{"name" => name, "id" => id} = json_response(conn, 200)

      {:ok, function} = Consumers.get_function_for_account(account.id, id)
      assert function.account_id == account.id
      assert function.name == name
      assert function.type == "routing"
    end

    test "creating a function with duplicate name fails", %{conn: conn, account: account} do
      FunctionsFactory.insert_function!(account_id: account.id, name: "duplicate-name")

      conn =
        post(conn, ~p"/api/functions", %{
          name: "duplicate-name",
          type: "filter",
          code: "def filter(action, record, changes, metadata), do: true"
        })

      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns validation error for invalid attributes", %{conn: conn} do
      invalid_attrs = %{name: nil}
      conn = post(conn, ~p"/api/functions", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      account: account,
      other_account: other_account
    } do
      function_attrs = %{
        name: "my-function",
        type: "filter",
        code: "def filter(action, record, changes, metadata), do: true",
        account_id: other_account.id
      }

      conn = post(conn, ~p"/api/functions", function_attrs)

      assert %{"id" => id} = json_response(conn, 200)

      {:ok, function} = Consumers.get_function_for_account(account.id, id)
      assert function.account_id == account.id
      assert function.account_id != other_account.id
    end
  end

  describe "update" do
    test "updates the function with valid attributes by id", %{conn: conn, function: function} do
      update_attrs = %{description: "Updated description"}
      conn = put(conn, ~p"/api/functions/#{function.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_function} = Consumers.get_function_for_account(function.account_id, id)
      assert updated_function.description == "Updated description"
      assert updated_function.name == function.name
    end

    test "updates the function with valid attributes by name", %{conn: conn, function: function} do
      update_attrs = %{description: "Updated description via name"}
      conn = put(conn, ~p"/api/functions/#{function.name}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_function} = Consumers.get_function_for_account(function.account_id, id)
      assert updated_function.description == "Updated description via name"
    end

    test "returns validation error for invalid attributes", %{conn: conn, function: function} do
      invalid_attrs = %{name: ""}
      conn = put(conn, ~p"/api/functions/#{function.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if function belongs to another account", %{
      conn: conn,
      other_function: other_function
    } do
      conn = put(conn, ~p"/api/functions/#{other_function.id}", %{description: "New description"})
      assert json_response(conn, 404)
    end
  end

  describe "delete" do
    test "deletes the function by id", %{conn: conn, function: function} do
      conn = delete(conn, ~p"/api/functions/#{function.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Consumers.get_function_for_account(function.account_id, id)
    end

    test "deletes the function by name", %{conn: conn, function: function} do
      conn = delete(conn, ~p"/api/functions/#{function.name}")
      assert %{"id" => _id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Consumers.find_function(function.account_id, name: function.name)
    end

    test "returns 404 if function belongs to another account", %{
      conn: conn,
      other_function: other_function
    } do
      conn = delete(conn, ~p"/api/functions/#{other_function.id}")
      assert json_response(conn, 404)
    end
  end
end
