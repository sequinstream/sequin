defmodule SequinWeb.PullController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.Error
  alias Sequin.String, as: SequinString
  alias Sequin.Time
  alias Sequin.Tracer
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def receive(conn, %{"id_or_name" => id_or_name} = params) do
    Logger.metadata(consumer_id: id_or_name)
    account_id = conn.assigns.account_id

    # TODO: Cache this
    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name, type: :sequin_stream),
         {:ok, batch_size} <- parse_batch_size(params),
         :ok <- maybe_wait(params, consumer),
         {:ok, messages} <- SlotMessageStore.produce(consumer.id, batch_size, self()) do
      Logger.metadata(batch_size: batch_size)
      Tracer.Server.messages_received(consumer, messages)
      render(conn, "receive.json", messages: messages)
    end
  end

  def ack(conn, %{"id_or_name" => id_or_name} = params) do
    Logger.metadata(consumer_id: id_or_name)
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name, type: :sequin_stream),
         {:ok, message_ids} <- parse_ack_ids(params),
         {:ok, _count} <- SlotMessageStore.ack(consumer, message_ids) do
      json(conn, %{success: true})
    end
  end

  def nack(conn, %{"id_or_name" => id_or_name} = params) do
    Logger.metadata(consumer_id: id_or_name)
    account_id = conn.assigns.account_id
    # now = DateTime.utc_now()

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: id_or_name, type: :sequin_stream),
         {:ok, ack_ids} <- parse_ack_ids(params),
         #  ack_ids_with_not_visible_until = Map.new(ack_ids, &{&1, now}),
         #  {:ok, _count} <- Consumers.nack_messages_with_backoff(consumer, ack_ids_with_not_visible_until),
         {:ok, _count} <- SlotMessageStore.ack(consumer, ack_ids) do
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

  # Old (deprecated) parameter name
  defp parse_batch_size(%{"batch_size" => batch_size}) do
    parse_batch_size(%{"max_batch_size" => batch_size})
  end

  defp parse_batch_size(%{"max_batch_size" => batch_size}) do
    with {:ok, int} <- maybe_parse_int(batch_size),
         true <- int > 0 and int <= 1000 do
      {:ok, int}
    else
      _ ->
        {:error,
         Error.bad_request(message: "Invalid `max_batch_size`. `max_batch_size` must be an integer between 1 and 1000.")}
    end
  end

  defp parse_batch_size(_params), do: {:ok, 1}

  # This is a silly way to respect the wait_for parameter. We'll make it more sophisticated soon.
  defp maybe_wait(%{"wait_for" => wait_for}, consumer) when is_binary(wait_for) do
    case Time.parse_duration(wait_for) do
      {:ok, ms} ->
        maybe_wait(%{"wait_for" => ms}, consumer)

      {:error, %Error.InvariantError{}} ->
        {:error,
         Error.bad_request(
           message: "Invalid `wait_for`. When using duration strings, format must be like '60s', '5m', or '1000ms'."
         )}
    end
  end

  defp maybe_wait(%{"wait_for" => wait_for}, consumer) when is_number(wait_for) do
    if wait_for >= min_wait_for() and wait_for <= :timer.minutes(5) do
      wait(consumer, wait_for)
    else
      {:error,
       Error.bad_request(
         message:
           "Invalid `wait_for`. Must be between #{min_wait_for()} and 300,000 milliseconds, or use duration format like '60s', '5m', '1000ms'."
       )}
    end
  end

  defp maybe_wait(_params, _consumer), do: :ok

  defp wait(consumer, wait_for) do
    {duration_us, count} = :timer.tc(fn -> SlotMessageStore.count_messages(consumer.id) end)

    duration = round(duration_us / 1000)
    wait_for = Enum.max([wait_for - duration, 0])

    cond do
      # We return if there's *anything* to deliver or we're out of wait time
      count > 0 or wait_for == 0 ->
        :ok

      wait_for <= wait_for_polling_interval() ->
        # Hope that when the wait elapses, we'll have enough messages
        Process.sleep(wait_for)
        :ok

      true ->
        Process.sleep(wait_for_polling_interval())
        wait(consumer, wait_for - wait_for_polling_interval())
    end
  end

  defp maybe_parse_int(int) when is_number(int) do
    {:ok, int}
  end

  defp maybe_parse_int(int) do
    case Integer.parse(int) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  defp min_wait_for do
    case env() do
      :test -> 1
      _ -> 500
    end
  end

  defp wait_for_polling_interval do
    case env() do
      :test -> 5
      _ -> 50
    end
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end
