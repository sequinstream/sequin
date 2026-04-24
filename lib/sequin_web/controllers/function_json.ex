defmodule SequinWeb.FunctionJSON do
  @doc """
  Renders a list of functions.
  """
  def index(%{functions: functions}) do
    %{data: for(function <- functions, do: render_one(function))}
  end

  @doc """
  Renders a single function.
  """
  def show(%{function: function}) do
    render_one(function)
  end

  @doc """
  Renders a deleted function.
  """
  def delete(%{function: function}) do
    %{id: function.id, deleted: true}
  end

  # `Sequin.Transforms.to_external/1` intentionally omits identity fields
  # (it's shared with config export, which is ID-agnostic). The management
  # API needs the id so clients can follow up with show/delete.
  defp render_one(function) do
    function
    |> Sequin.Transforms.to_external()
    |> Map.put(:id, function.id)
  end
end
