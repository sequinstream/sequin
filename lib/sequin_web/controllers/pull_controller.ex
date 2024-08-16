defmodule SequinWeb.PullController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Error
  alias Sequin.Streams
  alias Sequin.String, as: SequinString
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def receive(conn, %{"id_or_name" => id_or_name} = params) do
    Logger.metadata(consumer_id: id_or_name)
    account_id = conn.assigns.account_id

    # TODO: Cache this
    with {:ok, consumer} <- Consumers.get_consumer_for_account(account_id, id_or_name),
         {:ok, batch_size} <- parse_batch_size(params),
         {:ok, _wait_for} <- parse_wait_for(params),
         {:ok, messages} <- Consumers.receive_for_consumer(consumer, batch_size: batch_size) do
      Logger.metadata(batch_size: batch_size)
      render(conn, "receive.json", messages: messages)
    end
  end

  def ack(conn, %{"id_or_name" => id_or_name} = params) do
    Logger.metadata(consumer_id: id_or_name)
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.get_consumer_for_account(account_id, id_or_name),
         {:ok, message_ids} <- parse_ack_ids(params),
         :ok <- Streams.ack_messages(consumer, message_ids) do
      json(conn, %{success: true})
    end
  end

  def nack(conn, %{"id_or_name" => id_or_name} = params) do
    Logger.metadata(consumer_id: id_or_name)
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.get_consumer_for_account(account_id, id_or_name),
         {:ok, message_ids} <- parse_ack_ids(params),
         :ok <- Streams.nack_messages(consumer, message_ids) do
      json(conn, %{success: true})
    end
  end

  defp parse_ack_ids(params) do
    ack_ids = Map.get(params, "ack_ids")

    with true <- is_list(ack_ids),
         true <- length(ack_ids) > 0,
         true <- Enum.all?(ack_ids, &(is_binary(&1) and &1 != "")),
         true <- SequinString.all_uuids?(ack_ids) do
      {:ok, ack_ids}
    else
      _ ->
        {:error,
         Error.bad_request(
           message:
             "Invalid ack_ids. Must send a top-level `ack_ids` property that is a non-empty list of valid UUID strings"
         )}
    end
  end

  defp parse_batch_size(%{"batch_size" => batch_size}) do
    with {:ok, int} <- maybe_parse_int(batch_size),
         true <- int > 0 and int <= 1000 do
      {:ok, int}
    else
      _ ->
        {:error, Error.bad_request(message: "Invalid `batch_size`. `batch_size` must be an integer between 1 and 1000.")}
    end
  end

  defp parse_batch_size(_params), do: {:ok, 1}

  defp parse_wait_for(%{"wait_for" => _wait_for}) do
    # with {:ok, int} <- maybe_parse_int(wait_for),
    #      true <- int > 500 and int <= :timer.minutes(5) do
    #   {:ok, int}
    # else
    #   _ ->
    #     {:error,
    #      Error.bad_request(
    #        message: "Invalid `wait_for`. `wait_for` must be an integer between 500 and 300,000 (milliseconds)."
    #      )}
    # end
    {:error,
     Error.bad_request(
       message: "This endpoint doesn't support the `wait_for` param at the moment. Please ask the Sequin team for help."
     )}
  end

  defp parse_wait_for(_params), do: {:ok, 0}

  defp maybe_parse_int(int) when is_number(int) do
    {:ok, int}
  end

  defp maybe_parse_int(int) do
    case Integer.parse(int) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end
end
