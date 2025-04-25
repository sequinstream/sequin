defmodule Sequin.RoutingTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.RoutingTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.Transforms.MiniElixir

  describe "routing transform creation and validation" do
    setup do
      account = AccountsFactory.insert_account!()
      {:ok, %{account: account}}
    end
    
    test "creating a routing transform with valid code", %{account: account} do
      routing_code = """
      def route(action, record, changes, metadata) do
        %{
          method: "PATCH",
          endpoint_path: "/users/\#{record["id"]}"
        }
      end
      """

      assert {:ok, transform} = 
        Consumers.create_transform(
          account.id,
          %{
            name: Factory.unique_word(),
            transform: %{
              type: :routing,
              sink_type: :http_push,
              code: routing_code
            }
          }
        )

      assert %Transform{transform: %RoutingTransform{}} = transform
      assert transform.transform.sink_type == :http_push
      assert transform.transform.code == routing_code
    end

  end
  
  describe "using routing transforms in HTTP push pipeline" do
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

      {:ok, transform} = 
        Consumers.create_transform(
          account.id,
          %{
            name: Factory.unique_word(),
            transform: %{
              type: :routing,
              sink_type: :http_push,
              code: routing_code
            }
          }
        )

      {:ok, mod} = MiniElixir.create(transform.id, transform.transform.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :http_push,
          sink: %{type: :http_push, http_endpoint_id: http_endpoint.id},
          message_kind: :event,
          routing_id: transform.id,
        )

      %{
        consumer: consumer,
        transform: transform,
        http_endpoint: http_endpoint
      }

    end

    test "HTTP request uses routing transform to determine method and path", %{
      consumer: consumer,
      http_endpoint: http_endpoint
    } do
      test_pid = self()
      event = ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id, 
        action: :insert,
        data: ConsumersFactory.consumer_event_data(
          action: :insert,
          record: record = %{"id" => "xyz"}
        )
      )

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      start_supervised!({SinkPipeline, [
        consumer: consumer,
        req_opts: [adapter: adapter],
        producer: Broadway.DummyProducer,
        test_pid: test_pid
      ]})

      ref = Broadway.test_message(
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
