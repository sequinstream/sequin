defmodule SequinWeb.LocalTunnelJSON do
  def render("index.json", %{entities: entities}) do
    %{data: Enum.map(entities, &render_tunnel/1)}
  end

  defp render_tunnel(entity) do
    %{
      entity_id: entity.id,
      bastion_port: entity.port,
      entity_name: entity.name
    }
  end
end
