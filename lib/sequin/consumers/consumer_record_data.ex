defmodule Sequin.Consumers.ConsumerRecordData do
  @moduledoc false
  use TypedStruct

  defmodule Metadata do
    @moduledoc false
    use TypedStruct

    @derive Jason.Encoder
    typedstruct do
      field :table_schema, :string
      field :table_name, :string
      field :consumer, :map
    end
  end

  @derive Jason.Encoder
  typedstruct do
    field :record, map()
    field :metadata, Metadata.t()
  end
end
