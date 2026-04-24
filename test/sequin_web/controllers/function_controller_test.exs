defmodule SequinWeb.FunctionControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    function = FunctionsFactory.insert_function!(account_id: account.id)
    other_function = FunctionsFactory.insert_function!(account_id: other_account.id)

    %{function: function, other_function: other_function, other_account: other_account}
  end

  describe "index" do
    test "lists functions in the given account", %{conn: conn, account: account, function: function} do
      another_function = FunctionsFactory.insert_function!(account_id: account.id)

      conn = get(conn, ~p"/api/functions")
      assert %{"data" => functions} = json_response(conn, 200)
      assert length(functions) == 2
      ids = Enum.map(functions, & &1["id"])
      assert function.id in ids
      assert another_function.id in ids
    end

    test "does not list functions from another account", %{conn: conn, other_function: other_function} do
      conn = get(conn, ~p"/api/functions")
      assert %{"data" => functions} = json_response(conn, 200)
      refute Enum.any?(functions, &(&1["id"] == other_function.id))
    end
  end

  describe "show" do
    test "shows function details by id", %{conn: conn, function: function} do
      conn = get(conn, ~p"/api/functions/#{function.id}")
      assert json = json_response(conn, 200)
      assert json["name"] == function.name
    end

    test "shows function details by name", %{conn: conn, function: function} do
      conn = get(conn, ~p"/api/functions/#{function.name}")
      assert json = json_response(conn, 200)
      assert json["name"] == function.name
    end

    test "returns 404 if function belongs to another account", %{conn: conn, other_function: other_function} do
      conn = get(conn, ~p"/api/functions/#{other_function.id}")
      assert json_response(conn, 404)
    end
  end

  describe "delete" do
    test "deletes the function", %{conn: conn, function: function} do
      conn = delete(conn, ~p"/api/functions/#{function.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Consumers.find_function(function.account_id, id: id)
    end

    test "deletes by name", %{conn: conn, function: function} do
      conn = delete(conn, ~p"/api/functions/#{function.name}")
      assert %{"deleted" => true} = json_response(conn, 200)
    end

    test "returns 404 if function belongs to another account", %{conn: conn, other_function: other_function} do
      conn = delete(conn, ~p"/api/functions/#{other_function.id}")
      assert json_response(conn, 404)
    end

    test "refuses to delete a function still referenced by a sink", %{conn: conn, account: account, function: function} do
      sink = ConsumersFactory.insert_sink_consumer!(account_id: account.id)
      {:ok, _sink} = Consumers.update_sink_consumer(sink, %{transform_id: function.id})

      conn = delete(conn, ~p"/api/functions/#{function.id}")
      assert conn.status in 400..499

      # function must still exist
      assert {:ok, _} = Consumers.find_function(account.id, id: function.id)
    end
  end
end
