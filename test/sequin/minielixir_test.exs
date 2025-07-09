defmodule Sequin.MiniElixirTest do
  use ExUnit.Case

  alias Sequin.Functions.MiniElixir.Validator
  alias Sequin.Functions.MiniElixir.Validator.PatternChecker

  describe "unwrap" do
    test "unwrap extracts the single def or fails" do
      assert {:ok, _} = Validator.unwrap(quote(do: def(transform(action, record, changes, metadata), do: x)))
      assert {:ok, _} = Validator.unwrap(quote(do: def(transform(_action, _record, _changes, _metadata), do: x)))
      assert {:ok, _} = Validator.unwrap(quote(do: def(route(action, record, changes, metadata), do: x)))
      assert {:ok, _} = Validator.unwrap(quote(do: def(route(_action, _record, _changes, _metadata), do: x)))

      assert {:error, :validator, _} = Validator.unwrap(quote(do: def(f(a, b, c, d), do: x)))
      assert {:error, :validator, _} = Validator.unwrap(quote(do: def(transform(a, b, c, d), do: x)))
      assert {:error, :validator, _} = Validator.unwrap(quote(do: def(route(a, b, c, d), do: x)))

      assert {:error, :validator, _} =
               Validator.unwrap(
                 quote do
                   @before_compile {:erlang, :halt}
                   def transform(a, b, c, d) do
                     a
                   end
                 end
               )
    end
  end

  describe "validator" do
    test "rejects dangerous functions" do
      assert {:error, :validator, _} = Validator.check(quote do: :erlang.halt())
      assert {:error, :validator, _} = Validator.check(quote do: apply(:erlang, :halt, []))
      assert {:error, :validator, _} = Validator.check(quote do: Kernel.apply(:erlang, :halt, []))
    end

    test "allows various common syntax constructs" do
      assert :ok = Validator.check(quote do: map = %{a: 1, b: 2})
      assert :ok = Validator.check(quote do: <<1, 2, 3>>)
      assert :ok = Validator.check(quote do: "hello" <> " world")
      assert :ok = Validator.check(quote do: [1, 2] ++ [3, 4])
      assert :ok = Validator.check(quote do: [][3])
      assert :ok = Validator.check(quote do: String.upcase("value"))
      assert :ok = Validator.check(quote do: a = fn x -> x * 2 end)
    end

    test "allows if expressions" do
      if_ast =
        quote do
          if x > 10 do
            "big"
          else
            "small"
          end
        end

      assert :ok = Validator.check(if_ast)
    end

    test "allows nested" do
      if_nest =
        quote do
          if x > 10 do
            if y < 3 do
              :ok
            else
              :what
            end
          else
            "small"
          end
        end

      assert :ok = Validator.check(if_nest)
    end

    test "cond expressions" do
      good_cond =
        quote do
          cond do
            x < 0 -> "negative"
            x > 0 -> "positive"
            true -> "zero"
          end
        end

      evil_cond =
        quote do
          cond do
            :erlang.halt() -> "xd"
          end
        end

      assert :ok = Validator.check(good_cond)
      assert {:error, :validator, _} = Validator.check(evil_cond)
    end

    test "case expressions" do
      good_case =
        quote do
          case [1, 2, 3] do
            x -> y
            _ -> :ok
          end
        end

      assert :ok = Validator.check(good_case)
    end

    test "funs" do
      fn_ast =
        quote do
          helper = fn x -> 1 + x end
          helper.(2)
        end

      assert :ok = Validator.check(fn_ast)

      fn_multi =
        quote do
          fn
            1 -> 2
            2 -> 3
          end
        end

      assert :ok = Validator.check(fn_multi)

      fn_evil =
        quote do
          fn
            1 -> :erlang.halt()
            2 -> 3
          end
        end

      assert {:error, :validator, _} = Validator.check(fn_evil)
    end

    test "indirect call" do
      ind_ast =
        quote do
          xd = :erlang
          xd.halt()
        end

      assert {:error, :validator, _} = Validator.check(ind_ast)
    end

    test "__info__" do
      ind_ast =
        quote do
          z = String.__info__(:attributes)
        end

      assert {:error, :validator, _} = Validator.check(ind_ast)
    end

    test "record dot access warning" do
      record_dot =
        quote do
          record.user.name
        end

      {:error, :validator, msg} = Validator.check(record_dot)
      assert msg =~ ~s(`record` fields must be accessed with the [] operator: record["user"]["name"])
    end

    test "tuples of different lengths" do
      # Tuple of length 1
      assert :ok = Validator.check(quote do: {1})

      # Tuple of length 2
      assert :ok = Validator.check(quote do: {1, 2})

      # Tuple of length 3
      assert :ok = Validator.check(quote do: {1, 2, 3})

      # Tuple of length 4
      assert :ok = Validator.check(quote do: {1, 2, 3, 4})

      # Nested tuples
      assert :ok = Validator.check(quote do: {{1, 2}, {3, 4}})
    end

    test "match? operator" do
      # Simple match
      assert :ok = Validator.check(quote do: match?(1, 1))

      # Pattern matching
      assert :ok = Validator.check(quote do: match?({x, y}, {1, 2}))

      # Struct comparison
      assert :ok = Validator.check(quote do: match?(~U[2025-04-17 20:32:20.228377Z], DateTime))

      # With guards
      assert :ok = Validator.check(quote do: match?(x when x > 0, 1))

      # In if condition
      assert :ok = Validator.check(quote do: if(match?(x when x > 0, 1), do: :ok, else: :error))
    end

    test "interpolation" do
      assert :ok = Validator.check(quote do: "#{1}")
    end

    test "attempt to access system environment" do
      assert {:error, :validator, _} = Validator.check(quote do: System.get_env("SECRET_KEY"))
      assert {:error, :validator, _} = Validator.check(quote do: System.cmd("ls", ["-la"]))
    end

    test "attempt to access file system" do
      assert {:error, :validator, _} = Validator.check(quote do: File.read("/etc/passwd"))
      assert {:error, :validator, _} = Validator.check(quote do: File.rm_rf("/"))
    end

    test "attempt to spawn processes" do
      assert {:error, :validator, _} = Validator.check(quote do: spawn(fn -> :erlang.halt() end))
      assert {:error, :validator, _} = Validator.check(quote do: Task.async(fn -> :erlang.halt() end))
    end

    test "attempt to use eval" do
      assert {:error, :validator, _} = Validator.check(quote do: Code.eval_string(":erlang.halt()"))
      assert {:error, :validator, _} = Validator.check(quote do: Code.eval_quoted(quote(do: :erlang.halt())))
    end

    test "attempt to use macros" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   defmacro evil() do
                     quote(do: :erlang.halt())
                   end
                 end
               )
    end

    test "attempt to use ports" do
      assert {:error, :validator, _} = Validator.check(quote(do: Port.open({:spawn, "rm -rf /"}, [:binary])))
    end

    test "attempt to use ETS" do
      assert {:error, :validator, _} = Validator.check(quote(do: :ets.new(:table, [:public])))
      assert {:error, :validator, _} = Validator.check(quote(do: :ets.insert(:table, {:key, :value})))
    end

    test "attempt to use reflection" do
      assert {:error, :validator, _} = Validator.check(quote(do: Kernel.apply(String, :upcase, ["test"])))
      assert {:error, :validator, _} = Validator.check(quote(do: Kernel.function_exported?(String, :upcase, 1)))
    end

    test "attempt to use network" do
      assert {:error, :validator, _} = Validator.check(quote(do: :gen_tcp.connect(~c"localhost", 80, [])))

      assert {:error, :validator, _} =
               Validator.check(quote(do: :httpc.request(:get, {~c"http://example.com", []}, [], [])))
    end

    test "attempt to use process dictionary" do
      assert {:error, :validator, _} = Validator.check(quote(do: Process.put(:secret, "value")))
      assert {:error, :validator, _} = Validator.check(quote(do: Process.get(:secret)))
    end

    test "attempt to use list comprehensions for side effects" do
      assert {:error, :validator, _} = Validator.check(quote(do: for(x <- 1..10, do: :erlang.halt())))
    end

    test "attempt to use try/rescue for control flow manipulation" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   try do
                     :erlang.halt()
                   rescue
                     _ -> :ok
                   end
                 end
               )
    end

    test "attempt to define inline macro with malicious intent" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   defmodule EvilMacro do
                     @moduledoc false
                     defmacro __using__(_) do
                       quote do
                         def evil_function do
                           :erlang.halt()
                         end
                       end
                     end
                   end
                 end
               )
    end

    test "attempt to define direct inline macro" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   defmacro evil() do
                     quote(do: :erlang.halt())
                   end
                 end
               )
    end

    test "Allows kernel functions" do
      assert :ok = Validator.check(quote do: to_string(1))
    end

    # Note that you are only allowed to start from an approved root
    test "Allows dot access" do
      assert :ok = Validator.check(quote do: metadata.a.b.c.e)
    end

    # This is fine becasue if you HAD an evil function you can call it with Enum.map
    test "Allows fun.()" do
      assert :ok = Validator.check(quote do: fun.())
    end

    test "Disallows evil &capture" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   evil = &erlang.halt/0
                   evil.()
                 end
               )
    end

    test "allows benign &capture" do
      assert :ok =
               Validator.check(
                 quote do
                   Map.update(%{id: 1}, :id, nil, &(&1 + 1))
                 end
               )
    end

    test "can't assign evil via struct pattern" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   z = %{__struct__: :erlang}
                   %metadata{} = z
                   metadata.halt()
                 end
               )
    end

    test "Allows kernel guards" do
      assert :ok =
               Validator.check(
                 quote do
                   case 1 do
                     x when is_number(x) -> :ok
                     y when is_binary(y) -> :ok
                   end
                 end
               )
    end

    test "cannot shadow existing roots" do
      assert {:error, :validator, "can't assign to argument: metadata"} =
               Validator.check(
                 quote do
                   metadata = 1
                 end
               )

      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   [metadata] = 1
                 end
               )

      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   {1, metadata, 3} = 1
                 end
               )

      assert {:error, :validator, _} =
               Validator.check(
                 quote do
                   %{z => metadata} = 1
                 end
               )
    end

    test "can assign to variables" do
      assert :ok =
               Validator.check(
                 quote do
                   var = 1
                   %{key: var} = %{key: 3}
                   [var] = 7
                   <<1, 2, var::binary-size(8)>> = "lmao"
                   {1, var, 3} = {1, 2, 3}
                 end
               )
    end

    test "empty cond expression" do
      assert :ok =
               Validator.check(
                 quote do
                   cond do
                   end
                 end
               )
    end

    test "with expressions" do
      # Basic with expression
      good_with =
        quote do
          with {:ok, x} <- Map.get(record, "field1"),
               {:ok, y} <- Map.get(record, "field2") do
            x + y
          end
        end

      assert :ok = Validator.check(good_with)

      # With expression with else clause
      with_else =
        quote do
          with {:ok, x} <- Map.get(record, "field1"),
               {:ok, y} <- Map.get(record, "field2") do
            x + y
          else
            error -> {:error, error}
          end
        end

      assert :ok = Validator.check(with_else)

      # With expression with multiple else clauses
      with_multiple_else =
        quote do
          with {:ok, x} <- Map.get(record, "field1"),
               {:ok, y} <- Map.get(record, "field2") do
            x + y
          else
            {:error, reason} -> {:error, reason}
            :timeout -> {:error, :timeout}
            _ -> {:error, :unknown}
          end
        end

      assert :ok = Validator.check(with_multiple_else)

      # With expression with guard
      with_guard =
        quote do
          with {:ok, x} when x > 0 <- Map.get(record, "field1"),
               {:ok, y} when y < 10 <- Map.get(record, "field2") do
            x + y
          end
        end

      assert :ok = Validator.check(with_guard)

      # With expression that tries to shadow argument should fail
      bad_with =
        quote do
          with {:ok, record} <- Map.get(action, "data") do
            record
          end
        end

      assert {:error, :validator, msg} = Validator.check(bad_with)
      assert msg =~ "can't assign to argument: record"

      # With expression with dangerous function call should fail
      evil_with =
        quote do
          with {:ok, x} <- :erlang.halt() do
            x
          end
        end

      assert {:error, :validator, _} = Validator.check(evil_with)
    end

    test "list cons operator" do
      # Test that the list cons operator | works in patterns
      list_cons =
        quote do
          case [1, 2, 3] do
            [head | tail] -> {head, tail}
            [] -> :empty
          end
        end

      assert :ok = Validator.check(list_cons)

      # Test with with expression using list cons
      with_list_cons =
        quote do
          with {:ok, [first | rest]} <- Map.get(record, "items") do
            {first, length(rest)}
          end
        end

      assert :ok = Validator.check(with_list_cons)
    end

    test "with expression with plain expressions" do
      # Test with expression that has plain expressions (not just <-)
      with_plain_expressions =
        quote do
          x = 1 + 2
          y = x * 2

          with {:ok, z} <- Map.get(record, "value") do
            x + y + z
          end
        end

      assert :ok = Validator.check(with_plain_expressions)
    end
  end

  defp get_vars(ast) do
    {:ok, vars} = PatternChecker.extract_bound_vars(ast)
    Enum.sort(vars)
  end

  describe "pattern checker" do
    test "simple variable" do
      ast = quote do: x
      assert get_vars(ast) == [:x]
    end

    test "ignored variable _" do
      ast = quote do: _
      assert get_vars(ast) == []
    end

    test "ignored named variable _foo" do
      ast = quote do: _foo
      assert get_vars(ast) == []
    end

    test "literals do not bind variables" do
      assert get_vars(quote do: 123) == []
      assert get_vars(quote do: "hello") == []
      assert get_vars(quote do: :world) == []
      assert get_vars(quote do: true) == []
      assert get_vars(quote do: nil) == []
      assert get_vars(quote do: [1, 2, 3]) == []
      assert get_vars(quote do: {1, :a}) == []
      assert get_vars(quote do: %{a: 1, b: 2}) == []
      assert get_vars(quote do: <<1, 2>>) == []
    end

    test "list patterns" do
      ast = quote do: [a, b]
      assert get_vars(ast) == [:a, :b]

      ast = quote do: [h | t]
      assert get_vars(ast) == [:h, :t]

      ast = quote do: [x, _, [y | _z], k]
      # _z is ignored
      assert get_vars(ast) == [:k, :x, :y]

      ast = quote do: [1, var, "str"]
      assert get_vars(ast) == [:var]

      ast = quote do: []
      assert get_vars(ast) == []
    end

    test "tuple patterns" do
      ast = quote do: {a, b}
      assert get_vars(ast) == [:a, :b]

      ast = quote do: {x, {y, _}, z}
      assert get_vars(ast) == [:x, :y, :z]

      ast = quote do: {x, q = {y, _}, z}
      assert get_vars(ast) == [:q, :x, :y, :z]

      ast = quote do: {:ok, val}
      assert get_vars(ast) == [:val]

      ast = quote do: {}
      assert get_vars(ast) == []
    end

    test "map patterns" do
      ast = quote do: %{"key2" => val2, key1: val1}

      assert get_vars(ast) == [:val1, :val2]

      ast = quote do: %{a: x, b: %{c: y, d: _z}}
      assert get_vars(ast) == [:x, :y]

      ast = quote do: %{}
      assert get_vars(ast) == []

      ast = quote do: %{existing_var => new_val}
      assert get_vars(ast) == [:new_val]
    end

    test "match operator (:=)" do
      ast = quote do: a = b
      assert get_vars(ast) == [:a, :b]

      ast = quote do: x = {y, z}
      assert get_vars(ast) == [:x, :y, :z]

      ast = quote do: {p, q} = r
      assert get_vars(ast) == [:p, :q, :r]

      ast = quote do: val = 10
      assert get_vars(ast) == [:val]

      ast = quote do: first = second = {third, fourth}
      assert get_vars(ast) == [:first, :fourth, :second, :third]
    end

    test "binary patterns" do
      ast = quote do: <<x, y::size(8), z::binary>>
      assert get_vars(ast) == [:x, :y, :z]

      ast = quote do: <<head::integer, _rest::binary>>
      assert get_vars(ast) == [:head]

      ast = quote do: <<a, b::little-unsigned-integer-size(var_size)-unit(1)>>
      # var_size is part of type spec, not bound by this pattern
      assert get_vars(ast) == [:a, :b]

      ast = quote do: <<>>
      assert get_vars(ast) == []

      # 0 is literal
      ast = quote do: <<0, tail::binary>>
      assert get_vars(ast) == [:tail]
    end

    test "when clauses (guards)" do
      ast = quote do: x when is_integer(x) and x > 0
      assert get_vars(ast) == [:x]

      # suppose c is from an enclosing scope
      ast = quote do: {a, b} when a > c and map_size(b) == 2
      assert get_vars(ast) == [:a, :b]
    end

    test "pin operator (^)" do
      ast = quote do: ^pinned_var
      assert get_vars(ast) == []

      ast = quote do: [^a, b, ^c]
      assert get_vars(ast) == [:b]

      ast = quote do: %{key: ^val, other: new_val}
      assert get_vars(ast) == [:new_val]
    end

    test "struct patterns" do
      ast = quote do: %MyStruct{field1: f1, field2: _f2, field3: f3}
      assert get_vars(ast) == [:f1, :f3]

      ast = quote do: %s{field1: f1, field2: _f2, field3: f3}
      assert get_vars(ast) == [:f1, :f3, :s]
    end

    test "repeated variables (uniq)" do
      ast = quote do: {x, x, y, [x]}
      assert get_vars(ast) == [:x, :y]
    end

    test "deeply nested and mixed patterns" do
      ast =
        quote do
          {status, [%{data: data_val, meta: _} | _rest_list], error_code} =
            nil
          when status != :error and data_val > 0
        end

      assert get_vars(ast) == [:data_val, :error_code, :status]

      complex_pattern =
        quote do
          [first_el = {inner_a, _}, %{"payload" => payload_b, :options => [%Opt{opt_val: c} | _opts]}, _ | tail_d]
        end

      assert get_vars(complex_pattern) == [:c, :first_el, :inner_a, :payload_b, :tail_d]
    end
  end
end
