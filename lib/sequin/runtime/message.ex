defmodule Sequin.Runtime.SlotProcessor.Message do
  @moduledoc """
  This module provides structures of CDC changes.

  This file draws heavily from https://github.com/cainophile/cainophile
  License: https://github.com/cainophile/cainophile/blob/master/LICENSE
  """
  use TypedStruct

  alias __MODULE__

  require Protocol

  defmodule Field do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :column_name, String.t()
      field :column_attnum, integer()
      field :value, any()
    end
  end

  @derive {Jason.Encoder, except: [:subscription_ids]}
  @derive {Inspect, except: [:fields]}
  typedstruct do
    field :action, :insert | :update | :delete
    field :columns, list(map())
    field :commit_timestamp, DateTime.t()
    field :commit_lsn, integer()
    field :commit_idx, integer()
    field :transaction_annotations, String.t()
    field :errors, any()
    field :ids, list()
    field :table_schema, String.t()
    field :table_name, String.t()
    field :table_oid, integer()
    field :trace_id, String.t()
    field :old_fields, list(Field.t())
    field :fields, list(Field.t())
    field :subscription_ids, list(String.t())
    field :byte_size, non_neg_integer()
    field :batch_epoch, non_neg_integer()
  end

  def message_fields(%Message{action: :insert} = message), do: message.fields
  def message_fields(%Message{action: :update} = message), do: message.fields
  def message_fields(%Message{action: :delete} = message), do: message.old_fields

  def fields_to_map(fields) do
    Map.new(fields, fn %{column_name: name, value: value} -> {name, value} end)
  end
end
