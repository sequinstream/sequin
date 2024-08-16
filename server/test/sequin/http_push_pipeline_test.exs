defmodule Sequin.StreamsRuntime.HttpPushPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.StreamsRuntime.HttpPushPipeline

  @moduletag skip: true

  setup do
    account = AccountsFactory.insert_account!()
    http_endpoint = StreamsFactory.insert_http_endpoint!(account_id: account.id)

    consumer =
      [account_id: account.id, http_endpoint_id: http_endpoint.id, kind: :push]
      |> StreamsFactory.insert_consumer!()
      |> Repo.preload(:http_endpoint)

    {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
  end

  test "messages are sent to the HTTP endpoint", %{consumer: consumer, http_endpoint: http_endpoint} do
    test_pid = self()
    message = StreamsFactory.insert_message!(stream_id: consumer.stream_id)

    adapter = fn %Req.Request{} = req ->
      assert to_string(req.url) == http_endpoint.base_url
      json = Jason.decode!(req.body)
      assert json["data"] == message.data
      assert json["key"] == message.key
      send(test_pid, :sent)
      {req, Req.Response.new(status: 200)}
    end

    start_pipeline!(consumer, adapter)

    ref = send_test_message(consumer, message)
    assert_receive {:ack, ^ref, [%{data: msg}], []}, 1_000
    assert is_struct(msg, Sequin.Streams.Message)
    assert msg.key == message.key
    assert_receive :sent, 1_000
  end

  @tag capture_log: true
  test "a non-200 response results in a failed message", %{consumer: consumer} do
    adapter = fn req ->
      {req, Req.Response.new(status: 500)}
    end

    start_pipeline!(consumer, adapter)

    ref = send_test_message(consumer)
    assert_receive {:ack, ^ref, [], [_failed]}, 2_000
  end

  @tag capture_log: true
  test "a transport error/timeout results in a failed message", %{consumer: consumer} do
    adapter = fn req ->
      {req, %Mint.TransportError{reason: :timeout}}
    end

    start_pipeline!(consumer, adapter)

    ref = send_test_message(consumer)
    assert_receive {:ack, ^ref, [], [_failed]}, 2_000
  end

  defp start_pipeline!(consumer, adapter) do
    start_supervised!(
      {HttpPushPipeline,
       [consumer: consumer, req_opts: [adapter: adapter], producer: Broadway.DummyProducer, test_pid: self()]}
    )
  end

  defp send_test_message(consumer, message \\ nil) do
    message = message || StreamsFactory.insert_message!(stream_id: consumer.stream_id)
    Broadway.test_message(broadway(consumer), message, metadata: %{topic: "test_topic", headers: []})
  end

  defp broadway(consumer) do
    HttpPushPipeline.via_tuple(consumer.id)
  end
end
