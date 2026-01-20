defmodule Sequin.RoutingTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline

  describe "routing function creation and validation" do
    setup do
      account = AccountsFactory.insert_account!()
      {:ok, %{account: account}}
    end

    test "creating a routing function with valid code", %{account: account} do
      routing_code = """
      def route(action, record, changes, metadata) do
        %{
          method: "PATCH",
          endpoint_path: "/users/\#{record["id"]}"
        }
      end
      """

      assert {:ok, function} =
               Consumers.create_function(
                 account.id,
                 %{
                   name: Factory.unique_word(),
                   function: %{
                     type: :routing,
                     sink_type: :http_push,
                     code: routing_code
                   }
                 }
               )

      assert %Function{function: %RoutingFunction{}} = function
      assert function.function.sink_type == :http_push
      assert function.function.code == routing_code
    end
  end

  describe "using routing functions in HTTP push pipeline" do
    setup do
      account = AccountsFactory.insert_account!()
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      routing_code = """
      def route(action, record, changes, metadata) do
        %{
          method: "PUT",
          endpoint_path: "/api/ROUTED/\#{record["id"]}"
        }
      end
      """

      {:ok, function} =
        Consumers.create_function(
          account.id,
          %{
            name: Factory.unique_word(),
            function: %{
              type: :routing,
              sink_type: :http_push,
              code: routing_code
            }
          }
        )

      {:ok, _} = MiniElixir.create(function.id, function.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :http_push,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id},
          routing_id: function.id,
          routing_mode: :dynamic
        )

      %{
        consumer: consumer,
        function: function,
        http_endpoint: http_endpoint
      }
    end

    test "HTTP request uses routing function to determine method and path", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
      test_pid = self()

      event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          action: :insert,
          data:
            ConsumersFactory.consumer_event_data(
              action: :insert,
              record: record = %{"id" => "xyz"}
            )
        )

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      start_supervised!(
        {SinkPipeline,
         [
           consumer_id: consumer.id,
           req_opts: [adapter: adapter],
           producer: Broadway.DummyProducer,
           test_pid: test_pid
         ]}
      )

      ref =
        Broadway.test_message(
          SinkPipeline.via_tuple(consumer.id),
          event,
          metadata: %{topic: "test_topic", headers: []}
        )

      assert_receive {:ack, ^ref, [%{data: %{data: %{action: :insert}}}], []}, 1_000
      assert_receive {:http_request, req}, 1_000

      # Verify that the request used the routing information
      assert req.method == "PUT"
      assert to_string(req.url) == "#{HttpEndpoint.url(http_endpoint)}/api/ROUTED/#{record["id"]}"
    end
  end
end
