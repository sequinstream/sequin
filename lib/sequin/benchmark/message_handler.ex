defmodule Sequin.Benchmark.MessageHandler do
  @moduledoc """
  A message handler for benchmarking that records stats instead of routing to consumers.

  Used with `--through sps` mode to measure pipeline performance through SlotProcessorServer
  without the overhead of SlotMessageStore and Broadway.
  """

  @behaviour Sequin.Runtime.MessageHandler

  alias Sequin.Benchmark.Profiler
  alias Sequin.Benchmark.Stats
  alias Sequin.Runtime.SlotProcessor.Message

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :partition_count, pos_integer(), enforce: true
      field :checksum_owner_id, term(), enforce: true
    end
  end

  @impl true
  def before_handle_messages(%Context{}, _messages), do: :ok

  @impl true
  def handle_messages(%Context{} = ctx, messages) do
    # Group by partition (using PK as group_id, same as BenchmarkSource)
    messages
    |> Enum.group_by(fn %Message{} = msg ->
      # Extract PK from message.ids (first element for single-column PK)
      group_id = msg.ids |> List.first() |> to_string()
      Stats.partition(group_id, ctx.partition_count)
    end)
    |> Enum.each(fn {partition, partition_msgs} ->
      # Sort within partition by (commit_lsn, commit_idx)
      partition_msgs
      |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})
      |> Enum.each(fn %Message{} = msg ->
        Stats.message_received(%Stats.Message{
          owner_id: ctx.checksum_owner_id,
          partition: partition,
          commit_lsn: msg.commit_lsn,
          commit_idx: msg.commit_idx,
          byte_size: msg.byte_size,
          created_at_us: extract_created_at(msg.fields)
        })
      end)
    end)

    if Profiler.enabled?() do
      Enum.each(messages, fn %Message{} = msg ->
        Profiler.checkpoint(msg.commit_lsn, msg.commit_idx, :sink_in)
        Profiler.finalize_message(msg.commit_lsn, msg.commit_idx)
      end)
    end

    {:ok, length(messages)}
  end

  @impl true
  def put_high_watermark_wal_cursor(%Context{}, _cursor), do: :ok

  # Also handle logical messages (no-op for benchmarking)
  def handle_logical_message(%Context{}, _commit_lsn, _msg), do: :ok

  # Extract created_at from message fields (stored as microseconds since epoch)
  defp extract_created_at(fields) when is_list(fields) do
    case Enum.find(fields, fn f -> f.column_name == "created_at" end) do
      %{value: created_at} when is_integer(created_at) ->
        created_at

      %{value: created_at} when is_binary(created_at) ->
        case Integer.parse(created_at) do
          {int, ""} -> int
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp extract_created_at(_), do: nil
end
