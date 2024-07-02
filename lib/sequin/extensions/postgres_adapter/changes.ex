defmodule Sequin.Extensions.PostgresAdapter.Changes do
  @moduledoc """
  This module provides structures of CDC changes.

  This file draws heavily from https://github.com/cainophile/cainophile
  License: https://github.com/cainophile/cainophile/blob/master/LICENSE
  """
  require Protocol

  defmodule(Transaction, do: defstruct([:changes, :commit_timestamp]))

  defmodule NewRecord do
    @moduledoc false
    @derive {Jason.Encoder, except: [:subscription_ids]}
    defstruct [
      :columns,
      :commit_timestamp,
      :errors,
      :schema,
      :table,
      :record,
      :subscription_ids,
      :type
    ]
  end

  defmodule UpdatedRecord do
    @moduledoc false
    @derive {Jason.Encoder, except: [:subscription_ids]}
    defstruct [
      :columns,
      :commit_timestamp,
      :errors,
      :schema,
      :table,
      :old_record,
      :record,
      :subscription_ids,
      :type
    ]
  end

  defmodule DeletedRecord do
    @moduledoc false
    @derive {Jason.Encoder, except: [:subscription_ids]}
    defstruct [
      :columns,
      :commit_timestamp,
      :errors,
      :schema,
      :table,
      :old_record,
      :subscription_ids,
      :type
    ]
  end

  defmodule(TruncatedRelation, do: defstruct([:type, :schema, :table, :commit_timestamp]))
end
