defmodule Sequin.StreamsRuntime.HttpPushPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.StreamsRuntime.HttpPushPipeline

  setup do
    account = AccountsFactory.insert_account!()
    http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

    consumer =
      [account_id: account.id, http_endpoint_id: http_endpoint.id, message_kind: :event]
      |> ConsumersFactory.insert_http_push_consumer!()
      |> Repo.preload(:http_endpoint)

    {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
  end

  test "events are sent to the HTTP endpoint", %{consumer: consumer, http_endpoint: http_endpoint} do
    test_pid = self()
    event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

    adapter = fn %Req.Request{} = req ->
      assert to_string(req.url) == http_endpoint.base_url
      json = Jason.decode!(req.body)

      assert json == %{
               "record" => event.data.record,
               "changes" => nil,
               "action" => "insert"
             }

      send(test_pid, :sent)
      {req, Req.Response.new(status: 200)}
    end

    start_pipeline!(consumer, adapter)

    ref = send_test_event(consumer, event.data)
    assert_receive {:ack, ^ref, [%{data: %{action: :insert}}], []}, 1_000
    assert_receive :sent, 1_000
  end

  @tag capture_log: true
  test "a non-200 response results in a failed event", %{consumer: consumer} do
    adapter = fn req ->
      {req, Req.Response.new(status: 500)}
    end

    start_pipeline!(consumer, adapter)

    ref = send_test_event(consumer)
    assert_receive {:ack, ^ref, [], [_failed]}, 2_000
  end

  @tag capture_log: true
  test "a transport error/timeout results in a failed event", %{consumer: consumer} do
    adapter = fn req ->
      {req, %Mint.TransportError{reason: :timeout}}
    end

    start_pipeline!(consumer, adapter)

    ref = send_test_event(consumer)
    assert_receive {:ack, ^ref, [], [_failed]}, 2_000
  end

  defp start_pipeline!(consumer, adapter) do
    start_supervised!(
      {HttpPushPipeline,
       [consumer: consumer, req_opts: [adapter: adapter], producer: Broadway.DummyProducer, test_pid: self()]}
    )
  end

  defp send_test_event(consumer, event \\ nil) do
    event = event || ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id).data
    Broadway.test_message(broadway(consumer), event, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    HttpPushPipeline.via_tuple(consumer.id)
  end
end
