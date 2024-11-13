defmodule Sequin.Factory.DestinationFactory do
  @moduledoc false

  import Sequin.Factory.Support

  alias Sequin.Factory

  def sqs_message(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %{
        id: Factory.uuid(),
        message_body: %{
          "event" => Factory.word(),
          "payload" => %{Factory.word() => Factory.word()}
        },
        attributes: %{
          Factory.word() => Factory.word()
        },
        message_group_id: Factory.unique_word(),
        message_deduplication_id: Factory.uuid()
      },
      attrs
    )
  end
end
