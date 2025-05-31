defmodule Sequin.IncidentIO do

  @severity %{
    minor: "01JTPQNMHZW8V4M7KTRS0JJ3T6",
    major: "01JTPQNMHZVDGBMXTZHN1FTJB5",
    critical: "01JTPQNMHZ3NNZFWW7EGSJSN9C",
  }

  # For reference
  @severities_api_response [
    %{
      "created_at" => "2025-05-08T01:20:29.766Z",
      "description" => "Issues with low impact, which can usually be handled within working hours. Most customers are unlikely to notice any problems. Examples include a slight drop in application performance.",
      "id" => "01JTPQNMHZW8V4M7KTRS0JJ3T6",
      "name" => "Minor",
      "rank" => 0,
      "updated_at" => "2025-05-08T01:20:29.766Z"
    },
    %{
      "created_at" => "2025-05-08T01:20:29.771Z",
      "description" => "Issues causing significant impact. Immediate response is usually required. We might have some workarounds that mitigate the impact on customers. Examples include an important sub-system failing.",
      "id" => "01JTPQNMHZVDGBMXTZHN1FTJB5",
      "name" => "Major",
      "rank" => 1,
      "updated_at" => "2025-05-08T01:20:29.771Z"
    },
    %{
      "created_at" => "2025-05-08T01:20:29.777Z",
      "description" => "Issues causing very high impact to customers. Immediate response is required. Examples include a full outage, or a data breach.",
      "id" => "01JTPQNMHZ3NNZFWW7EGSJSN9C",
      "name" => "Critical",
      "rank" => 2,
      "updated_at" => "2025-05-08T01:20:29.777Z"
    }
  ]

  
  # https://api-docs.incident.io/tag/Incidents-V2#operation/Incidents%20V2_Create

  def create_incident(severity, params) do
    key = Application.get_env(:sequin, :incident_io_api_key)
    body = %{
      visibility: "public",
    }
    |> Map.put(:severity_id, @severity[severity])
    |> Map.merge(if is_map(params), do: params, else: Map.new(params))
    
    Req.post("https://api.incident.io/v2/incidents", json: body, auth: {:bearer, config[:key]})
  end
end
