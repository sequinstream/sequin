defmodule SequinWeb.MessageController do
  use SequinWeb, :controller

  alias Sequin.Error
  alias Sequin.Streams
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def publish(conn, %{"stream_id" => stream_id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id),
         {:ok, messages} <- parse_messages(params),
         {:ok, count} <- Streams.upsert_messages(stream.id, messages) do
      render(conn, "publish.json", count: count)
    end
  end

  def stream_list(conn, %{"stream_id" => stream_id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id),
         {:ok, list_params} <- parse_stream_list_params(params) do
      messages = Streams.list_messages_for_stream(stream.id, list_params)
      render(conn, "stream_list.json", messages: messages)
    end
  end

  def consumer_list(conn, %{"stream_id" => stream_id, "consumer_id" => consumer_id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, _stream} <- Streams.get_stream_for_account(account_id, stream_id),
         {:ok, consumer} <- Streams.get_consumer_for_stream(stream_id, consumer_id),
         {:ok, list_params} <- parse_consumer_list_params(params) do
      consumer_messages = Streams.list_consumer_messages_for_consumer(stream_id, consumer.id, list_params)
      render(conn, "consumer_list.json", consumer_messages: consumer_messages)
    end
  end

  defp parse_messages(%{"messages" => messages}) when is_list(messages) do
    Enum.reduce_while(messages, {:ok, []}, fn message, {:ok, acc} ->
      case message do
        %{"subject" => subject, "data" => data} when is_binary(subject) and is_binary(data) ->
          {:cont, {:ok, [%{subject: subject, data: data} | acc]}}

        _ ->
          {:halt, {:error, Error.bad_request(message: "Invalid message format")}}
      end
    end)
  end

  defp parse_messages(_), do: {:error, Error.bad_request(message: "Missing or invalid messages")}

  defp parse_stream_list_params(params) do
    with {:ok, sort} <- parse_stream_sort(params),
         {:ok, limit} <- parse_limit(params) do
      {:ok, Sequin.Keyword.reject_nils(limit: limit, order_by: sort)}
    end
  end

  defp parse_consumer_list_params(params) do
    with {:ok, sort} <- parse_consumer_sort(params),
         {:ok, limit} <- parse_limit(params),
         {:ok, state} <- parse_state(params) do
      {:ok, Sequin.Keyword.reject_nils(limit: limit, order_by: sort, state: state)}
    end
  end

  defp parse_stream_sort(%{"sort" => "seq_asc"}), do: {:ok, asc: :seq}
  defp parse_stream_sort(%{"sort" => "seq_desc"}), do: {:ok, desc: :seq}
  defp parse_stream_sort(%{"sort" => _}), do: {:error, Error.bad_request(message: "Invalid sort parameter")}
  defp parse_stream_sort(_), do: {:ok, asc: :seq}

  defp parse_consumer_sort(%{"sort" => "seq_asc"}), do: {:ok, asc: :message_seq}
  defp parse_consumer_sort(%{"sort" => "seq_desc"}), do: {:ok, desc: :message_seq}
  defp parse_consumer_sort(%{"sort" => _}), do: {:error, Error.bad_request(message: "Invalid sort parameter")}
  defp parse_consumer_sort(_), do: {:ok, asc: :message_seq}

  defp parse_limit(%{"limit" => limit}) do
    case Integer.parse(limit) do
      {int, ""} when int > 0 and int <= 10_000 -> {:ok, int}
      _ -> {:error, Error.bad_request(message: "Invalid limit parameter")}
    end
  end

  defp parse_limit(_), do: {:ok, 100}

  defp parse_state(%{"state" => "available"}), do: {:ok, :available}
  defp parse_state(%{"state" => "delivered"}), do: {:ok, [:delivered, :pending_redelivery]}
  defp parse_state(%{"state" => _}), do: {:error, Error.bad_request(message: "Invalid state parameter")}
  defp parse_state(_), do: {:ok, nil}
end
