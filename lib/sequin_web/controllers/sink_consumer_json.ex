defmodule SequinWeb.SinkConsumerJSON do
  @doc """
  Renders a list of sink consumers.
  """
  def index(%{sink_consumers: sink_consumers}) do
    %{data: for(sink_consumer <- sink_consumers, do: Sequin.Transforms.to_external(sink_consumer))}
  end

  @doc """
  Renders a single sink consumer.
  """
  def show(%{sink_consumer: sink_consumer}) do
    Sequin.Transforms.to_external(sink_consumer)
  end

  @doc """
  Renders a deleted sink consumer.
  """
  def delete(%{sink_consumer: sink_consumer}) do
    %{id: sink_consumer.id, deleted: true}
  end
end
