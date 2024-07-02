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
    alias Sequin.JSON

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

    def from_json(json) do
      JSON.struct(json, __MODULE__)
    end
  end

  defmodule UpdatedRecord do
    @moduledoc false
    alias Sequin.JSON

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

    def from_json(json) do
      JSON.struct(json, __MODULE__)
    end
  end

  defmodule DeletedRecord do
    @moduledoc false
    alias Sequin.JSON

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

    def from_json(json) do
      JSON.struct(json, __MODULE__)
    end
  end

  defmodule(TruncatedRelation, do: defstruct([:type, :schema, :table, :commit_timestamp]))
end
