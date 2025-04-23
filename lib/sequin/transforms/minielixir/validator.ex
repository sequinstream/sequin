defmodule Sequin.Transforms.MiniElixir.Validator do
  @moduledoc false
  @allowed_funname [:transform, :route]
  @args [:action, :record, :changes, :metadata]
  @error_bad_toplevel "Expecting only `def transform` or `def route` at the top level"
  @error_invalid_name "Only function names `transform` or `route` are allowed"

  def create_expr(body_ast, modname, funname \\ :transform) do
    :ok = check(body_ast)

    arglist = Enum.map(@args, fn a -> Macro.var(a, :"Elixir") end)

    body_ast =
      Macro.prewalk(body_ast, fn
        {a, m, nil} when is_atom(a) and a in @args -> {a, m, :"Elixir"}
        e -> e
      end)

    quote do
      defmodule unquote(modname) do
        def unquote(funname)(unquote_splicing(arglist)) do
          unquote(body_ast)
        end
      end
    end
  end

  def unwrap({:def, _, [{fnname, _, params}, [{:do, body}]]}) do
    with :ok <- unwrap_fnname(fnname),
         :ok <- unwrap_params(params) do
      {:ok, body}
    end
  end

  def unwrap(_) do
    {:error, :validator, @error_bad_toplevel}
  end

  defp unwrap_fnname(fnname) when fnname in @allowed_funname, do: :ok
  defp unwrap_fnname(_), do: {:error, :validator, @error_invalid_name}

  defp unwrap_params(args) do
    case Enum.map(args, &elem(&1, 0)) do
      @args -> :ok
      _ -> {:error, :validator, "The parameter list `#{Enum.join(@args, ", ")}` is required"}
    end
  end

  @goodop [
    # note that e.g. <<0::99999>> creates large binary
    :"::",
    :<<>>,
    :++,
    :+,
    :-,
    :*,
    :/,
    :==,
    :!=,
    :===,
    :!==,
    :>,
    :>=,
    :<,
    :<=,
    :&&,
    :||,
    :and,
    :or,
    :in,
    :not,
    :<>,
    :.,
    :|>,
    :__block__,
    :->,
    :=,
    :fn,
    :{},
    :when
  ]

  @kernel_sigils ~w[
    sigil_C
    sigil_D
    sigil_N
    sigil_R
    sigil_S
    sigil_T
    sigil_U
    sigil_c
    sigil_r
    sigil_s
    sigil_w
  ]a

  @kernel_guards ~w[
    is_integer
    is_binary
    is_bitstring
    is_atom
    is_boolean
    is_integer
    is_float
    is_number
    is_list
    is_map
    is_map_key
    is_nil
    is_reference
    is_tuple
    is_exception
    is_struct
    is_function
  ]a

  @kernel_functions ~w[
    abs
    binary_part
    bit_size
    byte_size
    ceil
    div
    elem
    floor
    get_and_update_in
    get_in
    hd
    length
    make_ref
    map_size
    max
    min
    not
    pop_in
    put_elem
    put_in
    rem
    round
    self
    tl
    trunc
    tuple_size
    update_in
    to_string
  ]a

  def check(ast) do
    if Macro.quoted_literal?(ast) do
      :ok
    else
      good(ast)
    end
  end

  defp good({op, _, body}) when op in @goodop, do: check_body(body)

  defp good({:%{}, _, body}) do
    Enum.reduce_while(body, :ok, fn
      {k, v}, :ok ->
        with :ok <- check(k),
             :ok <- check(v) do
          {:cont, :ok}
        else
          error -> {:halt, error}
        end

      _, _acc ->
        {:halt, {:error, :validator, "improper map literal"}}
    end)
  end

  defp good({:if, _, [c, ks]}) do
    with :ok <- check(c),
         :ok <- check(ks[:do]) do
      check(ks[:else])
    end
  end

  defp good({:case, _, [e, [{:do, body}]]}) do
    with :ok <- check(e), do: check_body(body)
  end

  defp good({a, _, nil}) when is_atom(a), do: :ok
  defp good(body) when is_list(body), do: check_body(body)
  defp good({l, r}), do: with(:ok <- check(l), do: check(r))
  defp good({sigil, _, body}) when sigil in @kernel_sigils, do: check(body)
  defp good({kernel_function, l, r}) when kernel_function in @kernel_functions, do: with(:ok <- check(l), do: check(r))
  defp good({:match?, _, [l, r]}), do: with(:ok <- check(l), do: check(r))
  defp good({:defmodule, _, _}), do: {:error, :validator, "defining modules is not allowed"}
  defp good({:def, _, _}), do: {:error, :validator, "defining functions is not allowed"}
  defp good({:defp, _, _}), do: {:error, :validator, "defining functions is not allowed"}
  defp good({:cond, _meta, [[{:do, body}]]}), do: check_body(body)

  # Variable
  defp good({f, _, ctx}) when is_atom(f) and is_atom(ctx), do: :ok

  # Fun call f.(x)
  defp good({{:., _, [fun]}, _, body}) when is_list(body) do
    with :ok <- check(fun), do: check_body(body)
  end

  # Function call
  defp good({f, _, body}) when is_list(body) do
    path = dedot(f)

    with :ok <- fnok(path),
         :ok <- noinfo(path),
         :ok <- warn_record_dot_access(path) do
      check_body(body)
    end
  end

  defp good(bad), do: {:error, :validator, format_error(bad)}

  defp warn_record_dot_access([:record | path]) do
    good =
      Enum.map_join(path, "", fn field -> "[\"#{field}\"]" end)

    {:error, :validator, "`record` fields must be accessed with the [] operator: record#{good}"}
  end

  defp warn_record_dot_access(_), do: :ok

  defp check_body(items) do
    Enum.reduce_while(items, :ok, fn item, :ok ->
      case check(item) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp format_error(e) do
    "forbidden construct: #{inspect(elem(e, 0))} in #{inspect_node(e)}"
  end

  defp inspect_node(e) do
    e
    |> Code.quoted_to_algebra()
    |> Inspect.Algebra.format(:infinity)
  end

  # The reason to disallow __info__ is because it might let you find weird data gadgets
  defp noinfo(path) do
    if Enum.all?(path, fn e -> e != :__info__ end) do
      :ok
    else
      {:error, :validator, "__info__ is not allowed"}
    end
  end

  # Allowlist of remote function calls
  defp fnok([top | _]) when top in @args, do: :ok
  defp fnok([Kernel, f]) when f in @kernel_guards or f in @kernel_functions, do: :ok
  defp fnok([Access, :get]), do: :ok
  defp fnok([Map, _]), do: :ok
  defp fnok([String, f]) when f not in [:to_atom, :to_existing_atom], do: :ok
  defp fnok([Enum, _]), do: :ok
  defp fnok([Date, _]), do: :ok
  defp fnok([DateTime, _]), do: :ok
  defp fnok([NaiveDateTime, _]), do: :ok
  defp fnok([Decimal, _]), do: :ok
  defp fnok([URI, _]), do: :ok
  defp fnok([Base, _]), do: :ok
  defp fnok(p), do: {:error, :validator, "Forbidden function: #{redot(p)}"}

  # Convert left-associated instances of the . operator to a get_in path
  defp dedot(ast), do: dedot(ast, [])

  defp dedot({{:., _, [l, r]}, _, []}, acc), do: dedot(l, [r | acc])
  defp dedot({:., _, [l, r]}, acc), do: dedot(l, [r | acc])
  defp dedot({:__aliases__, _, [name]}, acc), do: [Module.concat([name]) | acc]
  defp dedot({name, _, _}, acc) when is_atom(name), do: [name | acc]
  defp dedot(l, acc) when is_atom(l), do: [l | acc]

  defp redot(xs), do: Enum.join(xs, ".")
end
