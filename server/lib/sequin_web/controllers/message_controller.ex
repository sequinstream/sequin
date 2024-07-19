defmodule SequinWeb.MessageController do
  use SequinWeb, :controller

  alias Sequin.Error
  alias Sequin.Streams
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def publish(conn, %{"stream_id_or_name" => stream_id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
         {:ok, messages} <- parse_messages(params),
         {:ok, count} <- Streams.upsert_messages(stream.id, messages) do
      render(conn, "publish.json", count: count)
    end
  end

  def stream_list(conn, %{"stream_id_or_name" => stream_id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
         {:ok, list_params} <- parse_stream_list_params(params) do
      messages = Streams.list_messages_for_stream(stream.id, list_params)
      render(conn, "stream_list.json", messages: messages)
    end
  end

  def stream_get(conn, %{"stream_id_or_name" => stream_id_or_name, "key" => key}) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
         {:ok, message} <- Streams.get_message_for_stream(stream.id, key) do
      render(conn, "stream_get.json", message: message)
    end
  end

  def stream_count(conn, %{"stream_id_or_name" => stream_id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
         {:ok, count_params} <- parse_stream_count_params(params) do
      count = Streams.count_messages_for_stream(stream.id, count_params)
      render(conn, "stream_count.json", count: count)
    end
  end

  def consumer_list(
        conn,
        %{"stream_id_or_name" => stream_id_or_name, "consumer_id_or_name" => consumer_id_or_name} = params
      ) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
         {:ok, consumer} <- Streams.get_consumer_for_stream(stream.id, consumer_id_or_name),
         {:ok, list_params} <- parse_consumer_list_params(params) do
      consumer_messages = Streams.list_consumer_messages_for_consumer(stream.id, consumer.id, list_params)
      render(conn, "consumer_list.json", consumer_messages: consumer_messages)
    end
  end

  def message_consumer_info(conn, %{"stream_id_or_name" => stream_id_or_slug, "key" => key}) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_slug),
         {:ok, message} <- Streams.get_message_for_stream(stream.id, key),
         {:ok, consumer_messages_details} <- Streams.get_consumer_details_for_message(key, stream.id) do
      render(conn, "message_info.json", message: message, consumer_messages_details: consumer_messages_details)
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
         {:ok, limit} <- parse_limit(params),
         {:ok, subject_pattern} <- parse_subject_pattern(params) do
      {:ok, Sequin.Keyword.reject_nils(limit: limit, order_by: sort, subject_pattern: subject_pattern)}
    end
  end

  defp parse_consumer_list_params(params) do
    with {:ok, sort} <- parse_consumer_sort(params),
         {:ok, limit} <- parse_limit(params),
         {:ok, visible} <- parse_visible(params),
         {:ok, subject_pattern} <- parse_subject_pattern(params) do
      {:ok,
       Sequin.Keyword.reject_nils(limit: limit, order_by: sort, is_deliverable: visible, subject_pattern: subject_pattern)}
    end
  end

  defp parse_stream_count_params(params) do
    with {:ok, subject_pattern} <- parse_subject_pattern(params) do
      {:ok, Sequin.Keyword.reject_nils(subject_pattern: subject_pattern)}
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

  defp parse_visible(%{"visible" => visible}) when visible in [true, false], do: {:ok, visible}

  defp parse_visible(%{"visible" => visible}) when visible in ["true", "false"],
    do: {:ok, String.to_existing_atom(visible)}

  defp parse_visible(%{"visible" => _}), do: {:error, Error.bad_request(message: "Invalid visible parameter")}
  defp parse_visible(_), do: {:ok, nil}

  defp parse_subject_pattern(%{"subject_pattern" => pattern}) do
    case Sequin.Subject.validate_subject_pattern(pattern) do
      :ok -> {:ok, pattern}
      {:error, reason} -> {:error, Error.bad_request(message: "Invalid subject pattern: #{reason}")}
    end
  end

  defp parse_subject_pattern(_), do: {:ok, nil}
end
