defmodule Sequin.Bench.EndToEnd do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Accounts
  alias Sequin.Bench.Utils
  alias Sequin.Repo
  alias Sequin.Streams

  require Logger

  def run(opts \\ []) do
    current_message_count =
      Repo.one(from(m in Streams.Message, select: count(m.key)), timeout: :timer.minutes(1))

    {account, opts} =
      Keyword.pop_lazy(opts, :account, fn ->
        List.first(Accounts.list_accounts())
      end)

    {stream, opts} =
      Keyword.pop_lazy(opts, :stream, fn ->
        account.id |> Streams.list_streams_for_account() |> List.first()
      end)

    stream.id
    |> Streams.list_consumers_for_stream()
    |> Enum.each(&Streams.delete_consumer_with_lifecycle/1)

    consumers =
      Enum.map(1..10, fn _ ->
        {:ok, consumer} =
          Streams.create_consumer_for_account_with_lifecycle(account.id, %{
            stream_id: stream.id
          })

        consumer
      end)

    consumer = List.first(consumers)

    default_opts = [
      max_time_s: 10,
      warmup_time_s: 1,
      parallel: [10, 50],
      inputs: [
        {"batch_1", [1]},
        {"batch_10", [10]},
        {"batch_100", [100]}
      ]
    ]

    Sequin.Bench.run(
      [
        {"e2e_0M_upsert", fn batch_size -> upsert_messages(stream.id, batch_size) end},
        {"e2e_1M_receive", fn batch_size -> receive_and_ack(consumer, batch_size) end,
         before: fn _input ->
           if current_message_count < 1_000_000 do
             populate_messages(stream.id, 1_000_000)
           else
             Logger.info("Skipping, db already populated")
           end
         end},
        {"e2e_10M_receive", fn batch_size -> receive_and_ack(consumer, batch_size) end,
         before: fn _input ->
           if current_message_count < 8_000_000 do
             populate_messages(stream.id, 10_000_000)
           else
             Logger.info("Skipping, db already populated")
           end
         end},
        {"e2e_10M_10C_receive",
         fn batch_size ->
           consumer = Enum.random(consumers)
           receive_and_ack(consumer, batch_size)
         end}
      ],
      Keyword.merge(default_opts, opts)
    )
  end

  def run_throughput(opts \\ []) do
    current_message_count =
      Repo.one(from(m in Streams.Message, select: count(m.key)), timeout: :timer.minutes(1))

    {account, opts} =
      Keyword.pop_lazy(opts, :account, fn ->
        List.first(Accounts.list_accounts())
      end)

    {stream, opts} =
      Keyword.pop_lazy(opts, :stream, fn ->
        account.id |> Streams.list_streams_for_account() |> List.first()
      end)

    stream.id
    |> Streams.list_consumers_for_stream()
    |> Enum.each(&Streams.delete_consumer_with_lifecycle/1)

    Repo.delete_all(from(m in Streams.Message, where: is_nil(m.seq)))
    Repo.delete_all(from(m in Streams.ConsumerMessage))

    consumers =
      Enum.map(1..10, fn _ ->
        {:ok, consumer} =
          Streams.create_consumer_for_account_with_lifecycle(account.id, %{
            stream_id: stream.id
          })

        consumer
      end)

    consumer = List.first(consumers)

    # Keep pressure on the system - this produces ~500 inserts/sec
    # Task.Supervisor.async(Sequin.TaskSupervisor, fn ->
    #   # Create a stream that emits 10 items every second
    #   interval_stream =
    #     Stream.resource(
    #       fn -> 0 end,
    #       fn counter ->
    #         # 200ms interval for 5 ops/second
    #         Process.sleep(200)
    #         items = Enum.to_list(counter..(counter + 9))
    #         {items, counter + 10}
    #       end,
    #       fn _ -> :ok end
    #     )

    #   interval_stream
    #   |> Flow.from_enumerable(max_demand: 10, stages: 10)
    #   |> Flow.partition(stages: 10, max_demand: 1)
    #   |> Flow.map(fn _ -> upsert_messages(stream.id, 100) end)
    #   |> Flow.run()
    # end)

    default_opts = [
      max_time_s: 30,
      warmup_time_s: 5,
      parallel: [1, 10, 50, 100],
      # parallel: [10, 50, 100],
      inputs: [
        {"batch_1", [1]},
        {"batch_10", [10]},
        {"batch_100", [100]}
      ]
    ]

    {:ok, key_agent} = Agent.start_link(fn -> %{} end)

    Sequin.Bench.run(
      [
        {"e2e_10M_throughput",
         fn batch_size ->
           upsert_and_await(stream.id, consumer, batch_size, key_agent)
         end,
         before: fn _input ->
           if current_message_count < 8_000_000 do
             #  populate_messages(stream.id, 10_000_000)
           else
             Logger.info("Skipping, db already populated")
           end
         end},
        {"e2e_10M_throughput_10C",
         fn batch_size ->
           # Have this pid use a deterministic consumer
           consumer = Enum.at(consumers, :erlang.phash2(self(), 10))

           upsert_and_await(stream.id, consumer, batch_size, key_agent)
         end,
         before: fn _input ->
           if current_message_count < 8_000_000 do
             #  populate_messages(stream.id, 10_000_000)
           else
             Logger.info("Skipping, db already populated")
           end
         end}
      ],
      Keyword.merge(default_opts, opts)
    )

    GenServer.stop(key_agent)
  end

  def upsert_and_await(stream_id, consumer, batch_size, key_agent) do
    messages = messages(stream_id, batch_size)
    message = List.first(messages)
    key = message.key
    Agent.update(key_agent, fn keys -> Map.put(keys, key, true) end)

    upsert_messages(stream_id, messages)

    1..1
    |> Stream.cycle()
    |> Enum.reduce_while(nil, fn _, _ ->
      messages = receive_and_ack(consumer, 100)
      keys = Enum.map(messages, & &1.key)
      Agent.update(key_agent, fn k -> Map.drop(k, keys) end)

      if Agent.get(key_agent, fn k -> Map.get(k, key) end) do
        {:cont, :cont}
      else
        {:halt, :ok}
      end
    end)
  end

  def upsert_messages(stream_id, messages) when is_list(messages) do
    Streams.upsert_messages(stream_id, messages)
    messages
  end

  def upsert_messages(stream_id, batch_size) do
    messages =
      Enum.map(1..batch_size, fn _ ->
        %{
          key: key(),
          stream_id: stream_id,
          data: Utils.rand_string()
        }
      end)

    upsert_messages(stream_id, messages)
  end

  defp messages(stream_id, batch_size) do
    Enum.map(1..batch_size, fn _ ->
      %{
        key: key(),
        stream_id: stream_id,
        data: Utils.rand_string()
      }
    end)
  end

  def receive_and_ack(consumer, batch_size) do
    {:ok, messages} = Streams.receive_for_consumer(consumer, batch_size: batch_size)
    Streams.ack_messages(consumer.id, Enum.map(messages, & &1.ack_id))
    messages
  end

  def key do
    # Should be low enough entropy for high collisions
    30 |> :crypto.strong_rand_bytes() |> Base.encode64()
  end

  defp populate_messages(stream_id, count) do
    Logger.info("Seeding database with #{count} messages")
    batch_size = 100_000
    num_batches = div(count, batch_size)

    Enum.each(1..num_batches, fn _ ->
      query = """
      INSERT INTO #{Streams.stream_schema()}.messages (key, stream_id, data, data_hash, inserted_at, updated_at)
      SELECT
        encode(decode(substr(md5(random()::text || i::text), 1, 8), 'hex'), 'base64'),
        $1,
        substr(md5(random()::text), 1, 20),
        substr(md5(random()::text), 1, 20),
        now(),
        now()
      FROM generate_series(1, $2) i
      ON CONFLICT (stream_id, key) DO NOTHING
      """

      Repo.query!(query, [UUID.string_to_binary!(stream_id), batch_size], timeout: :timer.minutes(2))
    end)

    Logger.info("Seeded database with #{count} messages")
  end
end
