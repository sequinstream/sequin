defmodule SequinWeb.LocalTunnelJSON do
  alias Sequin.Accounts.LocalTunnel

  def render("index.json", %{tunnels: tunnels}) do
    %{data: Enum.map(tunnels, &render_tunnel/1)}
  end

  defp render_tunnel(%LocalTunnel{} = tunnel) do
    entity =
      case tunnel do
        %{http_endpoints: [http_endpoint]} -> http_endpoint
        %{postgres_databases: [postgres_database]} -> postgres_database
        _ -> nil
      end

    entity_id = if entity, do: entity.id
    entity_name = if entity, do: entity.name

    %{
      id: tunnel.id,
      description: tunnel.description,
      bastion_port: tunnel.bastion_port,
      entity_id: entity_id,
      entity_name: entity_name,
      inserted_at: tunnel.inserted_at,
      updated_at: tunnel.updated_at
    }
  end
end
