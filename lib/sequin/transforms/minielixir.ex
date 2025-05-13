defmodule Sequin.Transforms.MiniElixir do
  @moduledoc false
  use Agent

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.Transform
  alias Sequin.Error
  alias Sequin.Transforms.MiniElixir.Validator

  require Logger

  @timeout 1000

  def start_link(_opts) do
    Agent.start_link(fn -> :no_state end, name: __MODULE__)
  end

  def run_compiled(%Transform{account_id: account_id} = transform, data) do
    if Sequin.feature_enabled?(account_id, :function_transforms) do
      __MODULE__
      |> Task.async(:run_compiled_inner, [transform, data])
      |> Task.await(@timeout)
      |> case do
        {:ok, answer} -> answer
        {:error, error} -> raise error
        {:error, :validator, error} -> raise error
      end
    else
      raise Error.invariant(message: "Function transforms are not enabled. Talk to the Sequin team to enable them.")
    end
  end

  def run_interpreted(%Transform{account_id: account_id} = transform, data) do
    if Sequin.feature_enabled?(account_id, :function_transforms) do
      __MODULE__
      |> Task.async(:run_interpreted_inner, [transform, data])
      |> Task.await(@timeout)
      |> case do
        {:ok, answer} ->
          answer

        {:error, error} ->
          raise error

        {:error, :validator, error} ->
          raise error
      end
    else
      raise Error.invariant(message: "Function transforms are not enabled. Talk to the Sequin team to enable them.")
    end
  end

  def run_interpreted_inner(%Transform{id: id, transform: %_s{code: code}}, data) do
    changes =
      case data do
        %ConsumerRecordData{} -> %{}
        %ConsumerEventData{changes: changes} -> changes
      end

    bindings = [
      action: to_string(data.action),
      record: data.record,
      changes: changes,
      metadata: Sequin.Map.from_struct_deep(data.metadata)
    ]

    outer = Code.string_to_quoted!(code)

    with {:ok, ast} <- Validator.unwrap(outer),
         :ok <- Validator.check(ast) do
      {{answer, _newbindings, _newenv}, _dx} =
        Code.with_diagnostics(fn ->
          # TODO plumb dx
          Code.eval_quoted_with_env(ast, bindings, Code.env_for_eval([]))
        end)

      {:ok, answer}
    end
  rescue
    error ->
      :telemetry.execute([:minielixir, :interpret, :exception], %{id: id})
      Logger.error("[MiniElixir] run_interpreted error raised: #{Exception.message(error)}", transform_id: id)
      {:error, error}
  end

  def run_compiled_inner(%Transform{id: id}, data) do
    changes =
      case data do
        %ConsumerRecordData{} -> %{}
        %ConsumerEventData{changes: changes} -> changes
      end

    {:ok, mod} = ensure_code_is_loaded(id)
    {:ok, mod.run(to_string(data.action), data.record, changes, Sequin.Map.from_struct_deep(data.metadata))}
  rescue
    error ->
      :telemetry.execute([:minielixir, :compile, :exception], %{id: id})

      error =
        Sequin.Error.service(
          service: "transform",
          message: format_error(id, error, __STACKTRACE__)
        )

      Logger.error("[MiniElixir] Transform failed: #{Exception.message(error)}", transform_id: id)
      {:error, error}
  end

  def create(id, code) do
    top = Code.string_to_quoted!(code)
    mod = String.to_atom(generate_module_name(id))

    log_ast(top)

    with {:ok, body_ast} <- Validator.unwrap(top) do
      mod_ast = Validator.create_expr(body_ast, mod)
      compile_and_load!(mod_ast)
    end
  end

  def log_ast(ast) do
    alg =
      ast
      |> Code.quoted_to_algebra()
      |> Inspect.Algebra.format(:infinity)

    Logger.info(["[MiniElixir] Create transform module:\n", alg])
  end

  def compile_and_load!(ast) do
    {result, _messages} =
      Code.with_diagnostics(fn ->
        try do
          {:ok, Code.compile_quoted(ast)}
        rescue
          err -> {:error, err}
        end
      end)

    case result do
      {:ok, [{mod, bin}]} ->
        case :code.load_binary(mod, ~c"nowhere", bin) do
          {:module, mod} ->
            {:ok, mod}

          {:error, error} ->
            Logger.error("[MiniElixir] Error loading module: #{inspect(error)}")
            {:error, :cantload}
        end

      {:ok, xs} when is_list(xs) ->
        # You should not have been able to define more than one module
        {:error, :too_many_modules}
    end
  end

  defp ensure_code_is_loaded(id) do
    unless is_code_loaded(id) do
      __MODULE__
      |> Agent.get_and_update(fn state ->
        try do
          recreate(id)
          {:ok, state}
        rescue
          e -> {{:error, e}, state}
        end
      end)
      |> case do
        :ok ->
          :ok

        {:error, ex} ->
          raise ex
      end
    end

    module_name_from_id(id)
  end

  defp is_code_loaded(id) do
    with {:ok, mod} <- module_name_from_id(id),
         {:file, _} <- :code.is_loaded(mod) do
      true
    else
      _ -> false
    end
  end

  defp recreate(id) do
    with false <- is_code_loaded(id),
         {:ok, %Transform{} = transform} <- Consumers.get_transform(id) do
      create(transform.id, transform.transform.code)
    end
  end

  def module_name_from_id(id) do
    modname = generate_module_name(id)
    mod = String.to_existing_atom(modname)
    {:ok, mod}
  rescue
    _ -> {:error, :not_found}
  end

  @error_modules [
    ArgumentError,
    ArithmeticError,
    CaseClauseError,
    CompileError,
    RuntimeError,
    SyntaxError,
    TokenMissingError,
    UndefinedFunctionError,
    MismatchedDelimiterError,
    MatchError,
    KeyError,
    FunctionClauseError,
    Sequin.Error.InvariantError,
    Protocol.UndefinedError
  ]

  def encode_error(%Protocol.UndefinedError{protocol: Jason.Encoder}) do
    %{type: "JSON encoding error", info: %{description: "Return value is not JSON serializable"}}
  end

  def encode_error(%Protocol.UndefinedError{protocol: protocol, value: value}) do
    %{type: "Type mismatch", info: %{description: "Value #{inspect(value)} does not implement `#{inspect(protocol)}`"}}
  end

  def encode_error(%{__struct__: s} = e) when s in @error_modules do
    %{type: Atom.to_string(s), info: Map.drop(e, [:__struct__, :__exception__])}
  end

  def encode_error(error) when is_exception(error) do
    %{type: "Unknown error", info: %{description: Exception.message(error)}}
  end

  defp generate_module_name(id) when is_binary(id) do
    <<"UserTransform.", id::binary>>
  end

  defp format_error(id, error, stacktrace) do
    msg = Exception.message(error)

    with {:ok, mod} <- module_name_from_id(id),
         [info | _] <- for({^mod, _f, _a, info} <- stacktrace, do: info),
         line when is_integer(line) <- info[:line] do
      "#{msg} (line: #{line})"
    else
      _ -> msg
    end
  end
end
