defmodule Sequin.MiniElixirTest do
  use ExUnit.Case

  alias Sequin.Transforms.MiniElixir.Validator

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
                 quote(
                   do:
                     defmacro evil() do
                       quote(do: :erlang.halt())
                     end
                 )
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
                 quote(
                   do:
                     try do
                       :erlang.halt()
                     rescue
                       _ -> :ok
                     end
                 )
               )
    end

    test "attempt to define inline macro with malicious intent" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote(
                   do:
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
                 )
               )
    end

    test "attempt to define direct inline macro" do
      assert {:error, :validator, _} =
               Validator.check(
                 quote(
                   do:
                     defmacro evil() do
                       quote(do: :erlang.halt())
                     end
                 )
               )
    end
  end
end
