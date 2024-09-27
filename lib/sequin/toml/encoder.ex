defprotocol Sequin.TOML.Encoder do
  @moduledoc """
  Protocol controlling how a value is encoded to TOML.

  ## Deriving

  The protocol allows leveraging Elixir's `@derive` feature
  to simplify protocol implementation in trivial cases. Accepted
  options are:

    * `:only` - encodes only values of specified keys.
    * `:except` - encodes all struct fields except specified keys.

  By default, all keys except the `:__struct__` key are encoded.

  ## Example

  Let's assume a presence of the following struct:

      defmodule Test do
        defstruct [:foo, :bar, :baz]
      end

  If we were to call `@derive Sequin.TOML.Encoder` just before `defstruct`,
  an implementation similar to the following implementation would be generated:

      defimpl Sequin.TOML.Encoder, for: Test do
        def encode(value, opts) do
          Sequin.TOML.Encode.map(Map.take(value, [:foo, :bar, :baz]), opts)
        end
      end

  If we called `@derive {Sequin.TOML.Encoder, only: [:foo]}`, an implementation
  similar to the following implementation would be generated:

      defimpl Sequin.TOML.Encoder, for: Test do
        def encode(value, opts) do
          Sequin.TOML.Encode.map(Map.take(value, [:foo]), opts)
        end
      end

  If we called `@derive {Sequin.TOML.Encoder, except: [:foo]}`, an implementation
  similar to the following implementation would be generated:

      defimpl Sequin.TOML.Encoder, for: Test do
        def encode(value, opts) do
          Sequin.TOML.Encode.map(Map.take(value, [:bar, :baz]), opts)
        end
      end

  The actually generated implementations are more efficient computing some data
  during compilation similar to the macros from the `Sequin.TOML.Helpers` module.

  ## Explicit implementation

  If you wish to implement the protocol fully yourself, it is advised to
  use functions from the `Sequin.TOML.Encode` module to do the actual iodata
  generation - they are highly optimized and verified to always produce
  valid TOML.
  """

  @type t :: term
  @type opts :: Sequin.TOML.Encode.opts()

  @fallback_to_any true

  @doc """
  Encodes `value` to TOML.

  The argument `opts` is opaque - it can be passed to various functions in
  `Sequin.TOML.Encode` (or to the protocol function itself) for encoding values to TOML.
  """
  @spec encode(t, opts) :: iodata
  def encode(value, opts)
end

defimpl Sequin.TOML.Encoder, for: Any do
  defmacro __deriving__(module, struct, opts) do
    fields = fields_to_encode(struct, opts)
    kv = Enum.map(fields, &{&1, generated_var(&1, __MODULE__)})
    encode_args = []
    kv_iodata = Sequin.Codegen.build_kv_iodata(kv, encode_args)

    quote do
      defimpl Sequin.TOML.Encoder, for: unquote(module) do
        require Sequin.TOML.Helpers

        def encode(%{unquote_splicing(kv)}, _opts) do
          unquote(kv_iodata)
        end
      end
    end
  end

  defp generated_var(name, context) do
    {name, [generated: true], context}
  end

  def encode(%_{} = struct, _opts) do
    raise Protocol.UndefinedError,
      protocol: @protocol,
      value: struct,
      description: """
      Sequin.TOML.Encoder protocol must always be explicitly implemented.

      If you own the struct, you can derive the implementation specifying \
      which fields should be encoded to TOML:

          @derive {Sequin.TOML.Encoder, only: [....]}
          defstruct ...

      It is also possible to encode all fields, although this should be \
      used carefully to avoid accidentally leaking private information \
      when new fields are added:

          @derive Sequin.TOML.Encoder
          defstruct ...

      Finally, if you don't own the struct you want to encode to TOML, \
      you may use Protocol.derive/3 placed outside of any module:

          Protocol.derive(Sequin.TOML.Encoder, NameOfTheStruct, only: [...])
          Protocol.derive(Sequin.TOML.Encoder, NameOfTheStruct)
      """
  end

  def encode(value, _opts) do
    raise Protocol.UndefinedError,
      protocol: @protocol,
      value: value,
      description: "Sequin.TOML.Encoder protocol must always be explicitly implemented"
  end

  defp fields_to_encode(struct, opts) do
    fields = Map.keys(struct)

    cond do
      only = Keyword.get(opts, :only) ->
        case only -- fields do
          [] ->
            only

          error_keys ->
            raise ArgumentError,
                  "`:only` specified keys (#{inspect(error_keys)}) that are not defined in defstruct: " <>
                    "#{inspect(fields -- [:__struct__])}"
        end

      except = Keyword.get(opts, :except) ->
        case except -- fields do
          [] ->
            fields -- [:__struct__ | except]

          error_keys ->
            raise ArgumentError,
                  "`:except` specified keys (#{inspect(error_keys)}) that are not defined in defstruct: " <>
                    "#{inspect(fields -- [:__struct__])}"
        end

      true ->
        fields -- [:__struct__]
    end
  end
end

defimpl Sequin.TOML.Encoder, for: Atom do
  def encode(atom, opts) do
    Sequin.TOML.Encode.atom(atom, opts)
  end
end

defimpl Sequin.TOML.Encoder, for: Integer do
  def encode(integer, _opts) do
    Sequin.TOML.Encode.integer(integer)
  end
end

defimpl Sequin.TOML.Encoder, for: Float do
  def encode(float, _opts) do
    Sequin.TOML.Encode.float(float)
  end
end

defimpl Sequin.TOML.Encoder, for: List do
  def encode(list, opts) do
    Sequin.TOML.Encode.list(list, opts)
  end
end

defimpl Sequin.TOML.Encoder, for: Map do
  def encode(map, opts) do
    Sequin.TOML.Encode.map(map, opts)
  end
end

defimpl Sequin.TOML.Encoder, for: BitString do
  def encode(binary, opts) when is_binary(binary) do
    Sequin.TOML.Encode.string(binary, opts)
  end

  def encode(bitstring, _opts) do
    raise Protocol.UndefinedError,
      protocol: @protocol,
      value: bitstring,
      description: "cannot encode a bitstring to TOML"
  end
end

defimpl Sequin.TOML.Encoder, for: [Date, Time, NaiveDateTime, DateTime] do
  def encode(value, _opts) do
    Sequin.TOML.Encode.datetime(value)
  end
end

defimpl Sequin.TOML.Encoder, for: Decimal do
  def encode(value, _opts) do
    Sequin.TOML.Encode.decimal(value)
  end
end
