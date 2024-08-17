defmodule Sequin.Consumers.ConsumerRecordData do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerMessageMetadata

  @derive Jason.Encoder
  typedstruct do
    field :record, map()
    field :metadata, ConsumerMessageMetadata.t()
  end
end
