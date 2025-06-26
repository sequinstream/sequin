defmodule Sequin.Runtime.Routing.RoutedMessage do
  @moduledoc """
  Generic wrapper for routed messages contains the sink-specific routing_info and the transformed message
  """
  use TypedStruct

  typedstruct do
    field :routing_info, struct()
    field :transformed_message, any()
  end
end
