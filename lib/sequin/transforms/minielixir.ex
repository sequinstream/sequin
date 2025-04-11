defmodule Sequin.Transforms.MiniElixir do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.FunctionTransform
  alias Sequin.Consumers.Transform
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Transforms.MiniElixir.Validator

  require Logger

  @timeout 1000

  def on_create(%Transform{type: "routing"} = xf) do
    case create(xf.id, xf.transform.code) do
      {:ok, _} -> :ok
      err -> Logger.warning("Transform create failed", id: xf.id, err: err)
    end

    xf
  end

  def on_create(e), do: e

  def on_update(e), do: on_create(e)

  def run_compiled(transform, data) do
    if Sequin.feature_enabled?(:function_transforms) do
      __MODULE__
      |> Task.async(:run_compiled_inner, [transform, data])
      |> Task.await(@timeout)
      |> case do
        {:ok, answer} -> answer
        {:error, error} -> raise error
      end
    else
      raise Error.invariant(message: "Function transforms are not enabled. Talk to the Sequin team to enable them.")
    end
  end

  def run_interpreted(transform, data) do
    if Sequin.feature_enabled?(:function_transforms) do
      __MODULE__
      |> Task.async(:run_interpreted_inner, [transform, data])
      |> Task.await(@timeout)
      |> case do
        {:ok, answer} -> answer
        {:error, error} -> raise error
      end
    else
      raise Error.invariant(message: "Function transforms are not enabled. Talk to the Sequin team to enable them.")
    end
  end

  def run_interpreted_inner(%Transform{id: id, transform: %FunctionTransform{code: code}}, data) do
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

    {:ok, mod} = ensure(id)
    {:ok, mod.transform(to_string(data.action), data.record, changes, Sequin.Map.from_struct_deep(data.metadata))}
  rescue
    error ->
      :telemetry.execute([:minielixir, :compile, :exception], %{id: id})
      error = Sequin.Error.service(service: "transform", message: Exception.message(error))
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
    {result, _messages} = compile_quoted(ast)

    case result do
      {:ok, [{mod, bin}]} ->
        case :code.load_binary(mod, ~c"nowhere", bin) do
          {:module, mod} ->
            {:ok, mod}

          _ ->
            {:error, :cantload}
        end

      {:ok, xs} when is_list(xs) ->
        # You should not have been able to define more than one module
        {:error, :too_many_modules}
    end
  end

  def compile_quoted(ast) do
    Code.with_diagnostics(fn ->
      try do
        {:ok, Code.compile_quoted(ast)}
      rescue
        err -> {:error, err}
      end
    end)
  end

  def ensure(id) do
    with {:ok, mod} <- ensure_name(id),
         {:file, _} <- :code.is_loaded(mod) do
      {:ok, mod}
    else
      _ -> recreate(id)
    end
  end

  def recreate(id) do
    with {:ok, xf} <- get_transform_for_account(id) do
      create(xf.id, xf.transform.code)
    end
  end

  def get_transform_for_account(id) do
    id
    |> Transform.where_id()
    |> Repo.one()
    |> case do
      nil -> {:error, :not_found}
      transform -> {:ok, transform}
    end
  end

  def ensure_name(id) do
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
    Sequin.Error.InvariantError
  ]
  def encode_error(%{__struct__: s} = e) when s in @error_modules do
    %{type: Atom.to_string(s), info: Map.drop(e, [:__struct__, :__exception__])}
  end

  defp generate_module_name(id) when is_binary(id) do
    <<"UserTransform.", id::binary>>
  end
end
