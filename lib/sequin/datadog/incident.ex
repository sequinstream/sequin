defmodule Sequin.Datadog.Incident do
  @moduledoc """
  Struct representing a Datadog Incident.
  """

  use TypedStruct

  alias __MODULE__

  @incident_keys ~w(id title status created modified archived case_id customer_impact_duration customer_impact_end customer_impact_scope customer_impact_start customer_impacted detected fields non_datadog_creator notification_handles public_id resolved severity state time_to_detect time_to_internal_response time_to_repair time_to_resolve visibility)
  @derive Jason.Encoder
  typedstruct do
    field :id, String.t(), enforce: true
    field :title, String.t(), enforce: true
    field :status, String.t()
    field :created, DateTime.t()
    field :modified, DateTime.t()
    field :archived, DateTime.t()
    field :case_id, integer()
    field :customer_impact_duration, integer()
    field :customer_impact_end, DateTime.t()
    field :customer_impact_scope, String.t()
    field :customer_impact_start, DateTime.t()
    field :customer_impacted, boolean()
    field :detected, DateTime.t()
    field :fields, map()
    field :non_datadog_creator, map()
    field :notification_handles, list()
    field :public_id, integer()
    field :resolved, DateTime.t()
    field :severity, String.t()
    field :state, String.t()
    field :time_to_detect, integer()
    field :time_to_internal_response, integer()
    field :time_to_repair, integer()
    field :time_to_resolve, integer()
    field :visibility, String.t()
  end

  def from_json(map) do
    map
    |> Map.take(@incident_keys)
    |> Sequin.Map.atomize_keys()
    |> parse_timestamps()
    |> then(&struct(Incident, &1))
  end

  defp parse_timestamps(attrs) do
    timestamp_fields = [
      :created,
      :modified,
      :archived,
      :customer_impact_end,
      :customer_impact_start,
      :detected,
      :resolved
    ]

    Enum.reduce(timestamp_fields, attrs, fn field, acc ->
      case Map.get(acc, field) do
        nil -> acc
        value -> Map.put(acc, field, Sequin.Time.parse_timestamp!(value))
      end
    end)
  end
end
