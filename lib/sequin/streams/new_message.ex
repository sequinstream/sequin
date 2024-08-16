defmodule Sequin.Streams.NewMessage do
  @moduledoc """
  Rename this to Message once we're ready to replace the existing Message module.
  """

  use TypedStruct

  typedstruct enforce: true do
    field :data, map()
    field :recorded_at, DateTime.t()
    field :deleted, boolean()
    field :user_fields, map()
  end
end
