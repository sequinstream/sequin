defmodule Sequin.Factory.Datadog do
  @moduledoc false

  import Sequin.Factory.Support

  alias Sequin.Datadog.Incident
  alias Sequin.Factory

  def incident(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Incident{
        id: Factory.uuid(),
        title: "Test Incident #{Factory.sequence()}",
        status: Factory.one_of(["active", "stable", "resolved", "completed"]),
        created: Factory.timestamp_past(),
        modified: Factory.timestamp_past(),
        archived: Factory.timestamp_future(),
        case_id: Factory.integer(),
        customer_impact_duration: Factory.integer(),
        customer_impact_end: Factory.timestamp_future(),
        customer_impact_scope: Factory.word(),
        customer_impact_start: Factory.timestamp_past(),
        customer_impacted: Factory.boolean(),
        detected: Factory.timestamp_past(),
        fields: %{},
        non_datadog_creator: %{},
        notification_handles: [],
        public_id: Factory.integer(),
        resolved: Factory.timestamp_future(),
        severity: Factory.one_of(["UNKNOWN", "SEV-1", "SEV-2", "SEV-3", "SEV-4", "SEV-5"]),
        state: Factory.one_of(["active", "stable", "resolved", "completed"]),
        time_to_detect: Factory.integer(),
        time_to_internal_response: Factory.integer(),
        time_to_repair: Factory.integer(),
        time_to_resolve: Factory.integer(),
        visibility: Factory.one_of(["public", "private"])
      },
      attrs
    )
  end

  def incident_attrs(attrs \\ []) do
    attrs
    |> incident()
    |> Map.from_struct()
  end
end
