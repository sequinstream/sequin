defmodule Sequin.Runtime.KinesisPipeline do
  @moduledoc false
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Transforms.Message

  def send(%SinkConsumer{sink: %KinesisSink{} = sink} = consumer, messages) when is_list(messages) do
    client = KinesisSink.aws_client(sink)

    messages
    |> Stream.map(fn msg ->
      case Message.to_external(consumer, msg.data) do
        {:ok, data} -> {msg, data}
        {:error, reason} -> {msg, {:error, reason}}
      end
    end)
    |> Stream.filter(fn {_, data} -> !match?({:error, _}, data) end)
    |> Stream.map(fn {msg, data} ->
      partition_key = extract_partition_key(data, sink.partition_key_field)

      {msg, %{
        "Data" => Jason.encode!(data),
        "PartitionKey" => partition_key,
        "StreamName" => sink.stream_name
      }}
    end)
    |> SinkPipeline.batch_and_send_to_service("Kinesis", client, &put_records/2)
  end

  defp extract_partition_key(data, partition_key_field) do
    cond do
      is_map(data) && Map.has_key?(data, partition_key_field) ->
        to_string(data[partition_key_field])
      
      is_map(data) && Map.has_key?(data, String.to_atom(partition_key_field)) ->
        to_string(data[String.to_atom(partition_key_field)])
      
      true ->
        # If the key doesn't exist, generate a random partition key
        :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    end
  end

  defp put_records(client, records) do
    request_body = %{
      "Records" => records
    }

    case AWS.Kinesis.put_records(client, request_body) do
      {:ok, %{"FailedRecordCount" => 0}} ->
        {:ok, length(records)}

      {:ok, %{"FailedRecordCount" => failed_count, "Records" => failed_records}} ->
        failed_indices = failed_records
                         |> Enum.with_index()
                         |> Enum.filter(fn {record, _index} -> Map.has_key?(record, "ErrorCode") end)
                         |> Enum.map(fn {_record, index} -> index end)
        
        {:partial_error, length(records) - failed_count, failed_indices}

      {:error, error} ->
        {:error, error}
    end
  end
end
