defmodule Sequin.Consumers.ConsumerRecordData do
  @moduledoc false
  use TypedStruct

  defmodule Metadata do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :table_schema, :string
      field :table_name, :string
      field :commit_timestamp, :utc_datetime_usec
    end
  end

  @derive Jason.Encoder
  typedstruct do
    field :record, map()
    field :metadata, Metadata.t()
  end
end
