defmodule SequinWeb.StreamControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    stream = StreamsFactory.insert_stream!(account_id: account.id)
    other_stream = StreamsFactory.insert_stream!(account_id: other_account.id)
    %{stream: stream, other_stream: other_stream, other_account: other_account}
  end

  describe "index" do
    test "lists streams in the given account", %{conn: conn, account: account, stream: stream} do
      another_stream = StreamsFactory.insert_stream!(account_id: account.id)

      conn = get(conn, ~p"/api/streams")
      assert %{"data" => streams} = json_response(conn, 200)
      assert length(streams) == 2
      atomized_streams = Enum.map(streams, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([stream, another_stream], atomized_streams, &(&1.id == &2.id))
    end

    test "does not list streams from another account", %{conn: conn, other_stream: other_stream} do
      conn = get(conn, ~p"/api/streams")
      assert %{"data" => streams} = json_response(conn, 200)
      refute Enum.any?(streams, &(&1["id"] == other_stream.id))
      assert Enum.all?(streams, & &1["stats"])
    end
  end

  describe "show" do
    test "shows stream details", %{conn: conn, stream: stream} do
      conn = get(conn, ~p"/api/streams/#{stream.id}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(stream, atomized_response, [:id, :idx, :account_id])

      assert %{
               message_count: _,
               consumer_count: _,
               storage_size: _
             } = Sequin.Map.atomize_keys(atomized_response.stats)
    end

    test "shows stream details by name", %{conn: conn, stream: stream} do
      conn = get(conn, ~p"/api/streams/#{stream.name}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert atomized_response.id == stream.id
      assert atomized_response.name == stream.name
    end

    test "returns 404 if stream belongs to another account", %{conn: conn, other_stream: other_stream} do
      conn = get(conn, ~p"/api/streams/#{other_stream.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    setup do
      stream_attrs = StreamsFactory.stream_attrs()
      %{stream_attrs: stream_attrs}
    end

    test "creates a stream under the authenticated account", %{conn: conn, account: account, stream_attrs: stream_attrs} do
      conn = post(conn, ~p"/api/streams", stream_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, stream} = Streams.get_stream_for_account(account.id, id)
      assert stream.account_id == account.id
    end

    # test "returns validation error for invalid attributes", %{conn: conn} do
    #   # Assuming there's a validation on the Stream schema that we can trigger
    #   invalid_attrs = %{idx: "invalid"}
    #   conn = post(conn, ~p"/api/streams", invalid_attrs)
    #   assert json_response(conn, 422)["errors"] != %{}
    # end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      account: account,
      other_account: other_account,
      stream_attrs: stream_attrs
    } do
      conn = post(conn, ~p"/api/streams", %{stream_attrs | account_id: other_account.id})
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, stream} = Streams.get_stream_for_account(account.id, id)
      assert stream.account_id == account.id
      assert stream.account_id != other_account.id
    end
  end

  describe "delete" do
    test "deletes the stream", %{conn: conn, stream: stream} do
      conn = delete(conn, ~p"/api/streams/#{stream.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Streams.get_stream_for_account(stream.account_id, id)
    end

    test "returns 404 if stream belongs to another account", %{conn: conn, other_stream: other_stream} do
      conn = delete(conn, ~p"/api/streams/#{other_stream.id}")
      assert json_response(conn, 404)
    end
  end
end
