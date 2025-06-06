defmodule SequinWeb.SinkConsumerJSON do
  @doc """
  Renders a list of sink consumers.
  """
  def index(%{sink_consumers: sink_consumers}) do
    consumers =
      sink_consumers
      |> Enum.map(fn sink_consumer ->
        try do
          Sequin.Transforms.to_external(sink_consumer)
        rescue
          _ -> nil
        end
      end)
      |> Enum.filter(& &1)

    %{data: consumers}
  end

  @doc """
  Renders a single sink consumer.
  """
  def show(%{sink_consumer: sink_consumer}) do
    Sequin.Transforms.to_external(sink_consumer)
  end

  def render("error.json", %{error: error}) do
    %{success: false, error: error}
  end

  @doc """
  Renders a deleted sink consumer.
  """
  def delete(%{sink_consumer: sink_consumer}) do
    %{id: sink_consumer.id, deleted: true}
  end
end
