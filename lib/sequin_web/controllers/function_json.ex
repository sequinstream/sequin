defmodule SequinWeb.FunctionJSON do
  @doc """
  Renders a list of functions.
  """
  def index(%{functions: functions}) do
    %{data: for(function <- functions, do: Sequin.Transforms.to_external(function))}
  end

  @doc """
  Renders a single function.
  """
  def show(%{function: function}) do
    Sequin.Transforms.to_external(function)
  end

  @doc """
  Renders a deleted function.
  """
  def delete(%{function: function}) do
    %{id: function.id, deleted: true}
  end
end
