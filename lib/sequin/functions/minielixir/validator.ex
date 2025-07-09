defmodule Sequin.Functions.MiniElixir.Validator do
  @moduledoc false
  alias Sequin.Functions.MiniElixir.Validator.PatternChecker

  @allowed_funname [:transform, :route, :filter]
  @args [:action, :record, :changes, :metadata]
  @error_bad_toplevel "Expecting only `def transform`, `def route` or `def filter` at the top level"
  @error_bad_args "The parameter list `#{Enum.join(@args, ", ")}` is required"

  def create_expr(body_ast, modname) do
    case check(body_ast) do
      :ok -> :ok
      # You are supposed to have checked beforehand!
      {:error, :validator, msg} -> raise msg
    end

    arglist = Enum.map(@args, fn a -> Macro.var(a, :"Elixir") end)

    body_ast =
      Macro.prewalk(body_ast, fn
        {a, m, nil} when is_atom(a) and a in @args -> {a, m, :"Elixir"}
        e -> e
      end)

    quote do
      defmodule unquote(modname) do
        def run(unquote_splicing(arglist)) do
          unquote(body_ast)
          # this is important to propagate the stacktrace
        rescue
          ex ->
            reraise ex, __STACKTRACE__
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
  defp unwrap_fnname(_), do: {:error, :validator, @error_bad_toplevel}

  defp unwrap_params(args, acc \\ [])

  defp unwrap_params([], acc) do
    case Enum.reverse(acc) do
      @args -> :ok
      _otherwise -> {:error, :validator, @error_bad_args}
    end
  end

  for arg <- @args do
    defp unwrap_params([{unquote(arg), _meta, _context} | rest], acc) do
      unwrap_params(rest, [unquote(arg) | acc])
    end

    defp unwrap_params([{unquote(:"_#{arg}"), _meta, _context} | rest], acc) do
      unwrap_params(rest, [unquote(arg) | acc])
    end
  end

  defp unwrap_params(_args, _acc) do
    {:error, :validator, @error_bad_args}
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
    :|,
    :__block__,
    :->,
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

  defp good({:with, _, clauses}) do
    check_with_clauses(clauses)
  end

  defp good({a, _, nil}) when is_atom(a), do: :ok
  defp good(body) when is_list(body), do: check_body(body)
  defp good({l, r}), do: with(:ok <- check(l), do: check(r))
  defp good({sigil, _, body}) when sigil in @kernel_sigils, do: check(body)

  defp good({kernel_function, _, args}) when kernel_function in @kernel_functions, do: check_body(args)

  defp good({kernel_guard, _, args}) when kernel_guard in @kernel_guards, do: check_body(args)

  defp good({:match?, _, [l, r]}), do: with(:ok <- check(l), do: check(r))
  defp good({:defmodule, _, _}), do: {:error, :validator, "defining modules is not allowed"}
  defp good({:def, _, _}), do: {:error, :validator, "defining functions is not allowed"}
  defp good({:defp, _, _}), do: {:error, :validator, "defining functions is not allowed"}
  defp good({:cond, _meta, [[{:do, body}]]}) when is_list(body), do: check_body(body)
  # Empty cond has an empty block not an empty list
  defp good({:cond, _meta, [[{:do, body}]]}), do: check(body)

  defp good({:=, _meta, [l, r]}) do
    with {:ok, bound} <- PatternChecker.extract_bound_vars(l) do
      case Enum.find(bound, fn b -> b in @args end) do
        nil -> check(r)
        b -> {:error, :validator, "can't assign to argument: #{to_string(b)}"}
      end
    end
  end

  # Variable
  defp good({f, _, ctx}) when is_atom(f) and is_atom(ctx), do: :ok

  # Fun call f.(x)
  defp good({{:., _, [fun]}, _, body}) when is_list(body) do
    with :ok <- check(fun), do: check_body(body)
  end

  # Operators / not otherwise handled
  defp good({op, _, body}) when op in @goodop, do: check_body(body)

  # Function call
  defp good({f, _, body}) when is_list(body) do
    path = dedot(f)

    with :ok <- fnok(path),
         :ok <- noinfo(path),
         :ok <- warn_record_dot_access(path) do
      check_body(body)
    end
  end

  defp good(bad) do
    {:error, :validator, format_error(bad)}
  end

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
  defp fnok([UUID, _]), do: :ok
  defp fnok([JSON, _]), do: :ok
  defp fnok([Integer, _]), do: :ok
  defp fnok([Regex, _]), do: :ok
  defp fnok([Eden, _]), do: :ok
  defp fnok([List, _]), do: :ok
  defp fnok([:&]), do: :ok

  defp fnok(p) do
    {:error, :validator, "Forbidden function: #{redot(p)}"}
  end

  # Convert left-associated instances of the . operator to a get_in path
  defp dedot(ast), do: dedot(ast, [])

  defp dedot({{:., _, [l, r]}, _, []}, acc), do: dedot(l, [r | acc])
  defp dedot({:., _, [l, r]}, acc), do: dedot(l, [r | acc])
  defp dedot({:__aliases__, _, [name]}, acc), do: [Module.concat([name]) | acc]
  defp dedot({name, _, _}, acc) when is_atom(name), do: [name | acc]
  defp dedot(l, acc) when is_atom(l), do: [l | acc]

  defp redot(xs), do: Enum.join(xs, ".")

  defp check_with_clauses([]), do: :ok

  defp check_with_clauses([{:<-, _, [pattern, expr]} | rest]) do
    with {:ok, bound} <- PatternChecker.extract_bound_vars(pattern) do
      case Enum.find(bound, fn b -> b in @args end) do
        nil ->
          with :ok <- check(pattern),
               :ok <- check(expr) do
            check_with_clauses(rest)
          end

        b ->
          {:error, :validator, "can't assign to argument: #{to_string(b)}"}
      end
    end
  end

  defp check_with_clauses([keyword_list]) when is_list(keyword_list) do
    # Handle the do/else block at the end
    with :ok <- check(keyword_list[:do]) do
      case keyword_list[:else] do
        nil -> :ok
        else_clauses -> check_body(else_clauses)
      end
    end
  end

  defp check_with_clauses([expr | rest]) do
    with :ok <- check(expr) do
      check_with_clauses(rest)
    end
  end

  defmodule PatternChecker do
    @moduledoc false

    defmodule BadPattern do
      @moduledoc false
      defexception [:node]
      def message(me), do: "Bad pattern: #{inspect(me.node)}"
    end

    def extract_bound_vars(pattern) do
      {:ok, pattern |> extract_vars([]) |> Enum.uniq()}
    rescue
      ex in BadPattern ->
        {:error, Exception.message(ex)}
    end

    # Variable
    defp extract_vars({name, _meta, context}, acc) when is_atom(name) and is_atom(context) do
      # Skip underscore variables which don't actually bind
      case Atom.to_string(name) do
        <<"_", _::binary>> -> acc
        _ -> [name | acc]
      end
    end

    # Pin
    defp extract_vars({:^, _meta, [_pin]}, acc) do
      acc
    end

    # Sub-pattern matching
    defp extract_vars({:=, _meta, [left, right]}, acc) do
      acc = extract_vars(left, acc)
      extract_vars(right, acc)
    end

    # Struct patterns
    defp extract_vars({:%, _meta, [s, body]}, acc) do
      Enum.reduce([s, body], acc, &extract_vars/2)
    end

    # Map patterns
    defp extract_vars({:%{}, _meta, pairs}, acc) do
      Enum.reduce(pairs, acc, fn {_key, value}, acc ->
        extract_vars(value, acc)
      end)
    end

    # List patterns
    defp extract_vars(elements, acc) when is_list(elements) do
      Enum.reduce(elements, acc, &extract_vars/2)
    end

    # List cons pattern [head | tail]
    defp extract_vars({:|, _meta, [head, tail]}, acc) do
      acc = extract_vars(head, acc)
      extract_vars(tail, acc)
    end

    # Tuple patterns
    defp extract_vars({:{}, _meta, elements}, acc) do
      Enum.reduce(elements, acc, &extract_vars/2)
    end

    # 2-element tuple gotcha!
    defp extract_vars({x, y}, acc) do
      Enum.reduce([x, y], acc, &extract_vars/2)
    end

    # Binary patterns
    defp extract_vars({:<<>>, _meta, segments}, acc) do
      Enum.reduce(segments, acc, fn
        {:"::", _meta, [var, _type]}, inner_acc -> extract_vars(var, inner_acc)
        segment, inner_acc -> extract_vars(segment, inner_acc)
      end)
    end

    # Guards
    defp extract_vars({:when, _meta, [pattern, _guard]}, acc) do
      # guards can't introduce new bindings
      extract_vars(pattern, acc)
    end

    # Literals and other patterns don't bind variables
    defp extract_vars(literal, acc)
         when is_number(literal) or is_binary(literal) or is_boolean(literal) or is_nil(literal),
         do: acc

    defp extract_vars(e, acc) when is_number(e) or is_atom(e) or is_binary(e), do: acc
    defp extract_vars({:__aliases__, _, _}, acc), do: acc

    defp extract_vars(other, _acc), do: raise(BadPattern, node: other)
  end
end
