defmodule SequinWeb.HttpEndpointJSON do
  @doc """
  Renders a list of http endpoints.
  """
  def index(%{http_endpoints: http_endpoints}) do
    %{data: for(http_endpoint <- http_endpoints, do: Sequin.Transforms.to_external(http_endpoint))}
  end

  @doc """
  Renders a single http endpoint.
  """
  def show(%{http_endpoint: http_endpoint}) do
    Sequin.Transforms.to_external(http_endpoint)
  end

  @doc """
  Renders a deleted http endpoint.
  """
  def delete(%{http_endpoint: http_endpoint}) do
    %{id: http_endpoint.id, deleted: true}
  end
end
