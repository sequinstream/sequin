defmodule Scratch do
  @sink %{type: "http", endpoint: "localhost:8888"}
  @action "insert"
  @record %{id: 999, customer_id: 1234, inner: %{value: 4, email: "john.doe@example.com"}}
  @changes nil
  @metadata %{}

  def go() do
    s = sub()

    [{mod, beam}] = Code.compile_quoted(s)
    {:module, lmod} = :code.load_binary(mod, ~c"nowhere", beam)
    lmod.go(@sink, @action, @record, @changes, @metadata)
  end

  def once(lmod) do
    lmod.go(@sink, @action, @record, @changes, @metadata)
  end
  
  def check(ast) do
    if Macro.quoted_literal?(ast) do
      true
    else
      good(ast)
    end
  end

  @goodop [
    :"::", :<<>>,
    :+, :-, :*, :/,
    :==, :!=, :===, :!==, :>, :>=, :<, :<=,
    :in, :not,
    :<>,
    :.
  ]
  @goodref [:sink, :action, :record, :changes, :metadata]

  def good({op, _, body}) when op in @goodop, do: Enum.all?(body, &check/1)
  def good({:%{}, _, body}), do: Enum.all?(body, fn {k, v} -> check(k) && check(v) end)
  def good({a, _, nil}) when is_atom(a), do: true
  def good(body) when is_list(body), do: Enum.all?(body, &check/1)
  def good({f, _, body}) do
    Enum.all?(body, &check/1) && fnok(dedot(f))
  end

  def fnok([top | _]) when top in @goodref, do: true
  def fnok([Kernel, :to_string]), do: true

  def dedot(ast), do: dedot(ast, [])

  defp dedot({{:., _, [l, r]}, _, []}, acc), do: dedot(l, [r | acc])
  defp dedot({:., _, [l, r]}, acc), do: dedot(l, [r | acc])
  defp dedot({name, _, _}, acc) when is_atom(name), do: [name | acc]
  defp dedot(l, acc) when is_atom(l), do: [l | acc]

  def sub() do
    {:ok, skel} = Code.string_to_quoted(File.read!("skel.ex"))
    {:ok, ast} =  Code.string_to_quoted(File.read!("interp.txt"))

    IO.inspect(ast, label: "AST")
    # IO.inspect(good(ast), label: "good?")

    true = good(ast)
    
    Macro.prewalk(skel, fn
      {:@, _, [{:modname, _, nil}]} -> MyModule
      {:@, _, [{:body, _, nil}]} -> ast
      e -> e
    end)
  end
end
