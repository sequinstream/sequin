defmodule Sequin.Runtime.Trace do
  @moduledoc """
  Provides tracing functionality for consumer events through Phoenix PubSub.
  """

  require Logger

  @type content :: map() | {Req.Request.t(), Req.Response.t()} | {Req.Request.t(), any()}

  defmodule Event do
    @moduledoc """
    Represents a trace event with a message, content, and timestamp.
    """
    use TypedStruct

    alias Sequin.Error

    typedstruct do
      field :status, :info | :error | :warning
      field :message, String.t()
      field :req_request, Req.Request.t() | nil
      field :req_response, Req.Response.t() | nil
      field :error, Error.t() | nil
      field :extra, map(), default: %{}
      field :published_at, DateTime.t()
    end

    def to_external(%__MODULE__{} = event) do
      %{
        status: event.status,
        message: event.message,
        req_request: format_external(event.req_request),
        req_response: format_external(event.req_response),
        error: format_external(event.error),
        extra: Map.new(event.extra, fn {k, v} -> {k, format_external(v)} end),
        published_at: event.published_at
      }
    end

    defp format_external(nil), do: nil
    defp format_external(binary) when is_binary(binary), do: to_string(binary)

    defp format_external(%Req.Request{} = req) do
      %{
        method: req.method,
        url: format_req_url(req),
        headers: req.headers,
        body: format_external(req.body) || format_external(req.options[:json])
      }
    end

    defp format_external(%Req.Response{} = resp) do
      %{
        status: resp.status,
        headers: resp.headers,
        body: format_external(resp.body)
      }
    end

    defp format_external(%URI{} = uri), do: URI.to_string(uri)

    defp format_external(error) when is_exception(error) do
      Exception.message(error)
    end

    defp format_external(tuple) when is_tuple(tuple) do
      tuple
      |> Tuple.to_list()
      |> Enum.map(&format_external/1)
    end

    defp format_external(%Decimal{} = decimal) do
      Decimal.to_string(decimal)
    end

    defp format_external(map) when is_map(map) and not is_struct(map) do
      Map.new(map, fn {k, v} -> {k, format_external(v)} end)
    end

    defp format_external(unknown) do
      case Jason.encode(unknown) do
        {:ok, _} ->
          unknown

        {:error, error} ->
          Logger.warning("Failed to encode unknown value for format_external: #{inspect(unknown)}", error: error)
          inspect(unknown)
      end
    end

    defp format_req_url(%Req.Request{} = req) do
      base_url =
        case Req.Request.get_option(req, :base_url) do
          nil -> ""
          base_url when is_binary(base_url) -> base_url
          %URI{} = base_url -> URI.to_string(base_url)
        end

      url =
        case req.url do
          nil -> ""
          %URI{} = url -> URI.to_string(url)
          url when is_binary(url) -> url
        end

      base_url <> url
    end
  end

  @topic_prefix "sequin:trace:"

  @doc """
  Subscribes to trace events for a specific consumer.
  """
  @spec subscribe(String.t()) :: :ok | {:error, term()}
  def subscribe(consumer_id) when is_binary(consumer_id) do
    topic = topic(consumer_id)
    Phoenix.PubSub.subscribe(Sequin.PubSub, topic)
  end

  @doc """
  Unsubscribes from trace events for a specific consumer.
  """
  @spec unsubscribe(String.t()) :: :ok | {:error, term()}
  def unsubscribe(consumer_id) when is_binary(consumer_id) do
    topic = topic(consumer_id)
    Phoenix.PubSub.unsubscribe(Sequin.PubSub, topic)
  end

  @doc """
  Publishes an info trace event for a specific consumer.
  """
  @spec info(String.t(), Event.t()) :: :ok | {:error, term()}
  def info(consumer_id, event) do
    publish(consumer_id, %Event{event | status: :info})
  end

  @doc """
  Publishes a warning trace event for a specific consumer.
  """
  @spec warning(String.t(), Event.t()) :: :ok | {:error, term()}
  def warning(consumer_id, event) do
    publish(consumer_id, %Event{event | status: :warning})
  end

  @doc """
  Publishes an error trace event for a specific consumer.
  """
  @spec error(String.t(), Event.t()) :: :ok | {:error, term()}
  def error(consumer_id, event) do
    publish(consumer_id, %Event{event | status: :error})
  end

  @spec publish(String.t() | nil, Event.t()) :: :ok | {:error, term()}
  defp publish(nil, _event), do: :ok

  defp publish(consumer_id, event) when is_binary(consumer_id) do
    Phoenix.PubSub.broadcast(
      Sequin.PubSub,
      topic(consumer_id),
      {:trace_event, %Event{event | published_at: DateTime.utc_now()}}
    )
  end

  @doc """
  Returns the PubSub topic for a specific consumer's trace events.
  """
  @spec topic(String.t()) :: String.t()
  def topic(consumer_id) when is_binary(consumer_id) do
    @topic_prefix <> consumer_id
  end
end
