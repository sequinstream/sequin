defmodule Sequin.Runtime.Trace do
  @moduledoc """
  Provides tracing functionality for consumer events through Phoenix PubSub.
  """

  import Sequin.Error.Guards, only: [is_error: 1]

  require Logger

  @type content :: map() | {Req.Request.t(), Req.Response.t()} | {Req.Request.t(), any()}

  defmodule Event do
    @moduledoc """
    Represents a trace event with a message, content, and timestamp.
    """
    @type status :: :info | :error | :warning

    @type t :: %__MODULE__{
            status: status(),
            message: String.t(),
            content: map(),
            published_at: DateTime.t()
          }

    defstruct [:status, :message, :content, :published_at]
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
  @spec info(String.t(), String.t(), content()) :: :ok | {:error, term()}
  def info(consumer_id, message, content) do
    publish(consumer_id, :info, message, content)
  end

  @doc """
  Publishes a warning trace event for a specific consumer.
  """
  @spec warning(String.t(), String.t(), content()) :: :ok | {:error, term()}
  def warning(consumer_id, message, content) do
    publish(consumer_id, :warning, message, content)
  end

  @doc """
  Publishes an error trace event for a specific consumer.
  """
  @spec error(String.t(), String.t(), content()) :: :ok | {:error, term()}
  def error(consumer_id, message, content) do
    publish(consumer_id, :error, message, coerce_content(content))
  end

  @spec publish(String.t(), Event.status(), String.t(), map()) :: :ok | {:error, term()}
  defp publish(consumer_id, status, message, content)
       when is_binary(consumer_id) and is_binary(message) and is_map(content) do
    event = %Event{
      status: status,
      message: message,
      content: content,
      published_at: DateTime.utc_now()
    }

    topic = topic(consumer_id)
    Phoenix.PubSub.broadcast(Sequin.PubSub, topic, {:trace_event, event})
  end

  defp coerce_content(content) do
    case content do
      {%Req.Request{} = req, %Req.Response{} = res} ->
        %{http_request: coerce_content(req), http_response: coerce_content(res)}

      {%Req.Request{} = req, error} when is_error(error) ->
        %{http_request: coerce_content(req), error: coerce_content(error)}

      %Req.Request{} = req ->
        %{
          method: req.method,
          url: URI.to_string(req.url),
          headers: req.headers,
          body: to_string(req.body)
        }

      %Req.Response{} = resp ->
        %{
          status: resp.status,
          headers: resp.headers,
          body: inspect(resp.body)
        }

      error when is_error(error) ->
        Exception.message(error)

      content when is_map(content) ->
        content

      other ->
        Logger.warning("Invalid content type for trace event: #{inspect(other)}")
        nil
    end
  end

  @doc """
  Returns the PubSub topic for a specific consumer's trace events.
  """
  @spec topic(String.t()) :: String.t()
  def topic(consumer_id) when is_binary(consumer_id) do
    @topic_prefix <> consumer_id
  end
end
