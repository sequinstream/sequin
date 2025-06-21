defmodule Sequin.Factory.SinkFactory do
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

  def meilisearch_record(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %{
        id: Factory.uuid(),
        data: %{
          "event" => Factory.word(),
          "payload" => %{
            "id" => Factory.uuid(),
            Factory.word() => Factory.word()
          }
        }
      },
      attrs
    )
  end

  def sns_message(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %{
        message_id: Factory.uuid(),
        message: %{
          "event" => Factory.word(),
          "payload" => %{Factory.word() => Factory.word()}
        },
        message_group_id: Factory.unique_word(),
        message_deduplication_id: Factory.uuid()
      },
      attrs
    )
  end

  def kinesis_record(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %{
        data: %{
          "event" => Factory.word(),
          "payload" => %{Factory.word() => Factory.word()}
        },
        partition_key: Factory.unique_word()
      },
      attrs
    )
  end

  def s2_record(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %{
        body: %{"event" => Factory.word(), "payload" => %{Factory.word() => Factory.word()}}
      },
      attrs
    )
  end
end
