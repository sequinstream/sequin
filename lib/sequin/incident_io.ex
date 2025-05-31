defmodule Sequin.IncidentIO do
  @moduledoc false
  @severity %{
    minor: "01JTPQNMHZW8V4M7KTRS0JJ3T6",
    major: "01JTPQNMHZVDGBMXTZHN1FTJB5",
    critical: "01JTPQNMHZ3NNZFWW7EGSJSN9C"
  }

  # https://api-docs.incident.io/tag/Incidents-V2#operation/Incidents%20V2_Create

  def create_incident(severity, params) do
    key = Application.get_env(:sequin, :incident_io_api_key)

    body =
      %{
        visibility: "public"
      }
      |> Map.put(:severity_id, @severity[severity])
      |> Map.merge(if is_map(params), do: params, else: Map.new(params))

    Req.post("https://api.incident.io/v2/incidents", json: body, auth: {:bearer, key})
  end
end
