defmodule Sequin.DatabasesRuntime.SlotProcessor.Message do
  @moduledoc """
  This module provides structures of CDC changes.

  This file draws heavily from https://github.com/cainophile/cainophile
  License: https://github.com/cainophile/cainophile/blob/master/LICENSE
  """
  use TypedStruct

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
  typedstruct do
    field :action, :insert | :update | :delete
    field :columns, list(map())
    field :commit_timestamp, DateTime.t()
    field :commit_lsn, integer()
    field :commit_idx, integer()
    field :errors, any()
    field :ids, list()
    field :table_schema, String.t()
    field :table_name, String.t()
    field :table_oid, integer()
    field :trace_id, String.t()
    field :old_fields, list(Field.t())
    field :fields, list(Field.t())
    field :subscription_ids, list(String.t())
  end
end
