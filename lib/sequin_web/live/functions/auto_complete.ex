defmodule SequinWeb.FunctionLive.AutoComplete do
  @moduledoc false
  @reserved_words ~w(do if else end case cond for with fn)
  @function_completion_modules [
    Base,
    Date,
    DateTime,
    Decimal,
    Eden,
    Enum,
    Integer,
    JSON,
    Map,
    NaiveDateTime,
    Regex,
    String,
    Time,
    URI
  ]

  def all_completions do
    reserved_completions() ++ function_completions()
  end

  def reserved_completions do
    Enum.map(@reserved_words, &%{label: &1, type: "keyword"})
  end

  def function_completions do
    Enum.flat_map(@function_completion_modules, &function_completions/1)
  end

  defp function_completions(module) when is_atom(module) do
    case Code.fetch_docs(module) do
      {:docs_v1, _, _, _, %{"en" => module_doc}, _, functions_with_docs} ->
        [first_sentence | _] = String.split(module_doc, ".")
        first_sentence = String.replace(first_sentence, "\n", "")

        module_completion = %{label: "#{inspect(module)}", type: "module", info: first_sentence}

        function_completions =
          functions_with_docs
          |> Enum.filter(fn
            {{:function, :__struct__, _arity}, _, _, %{}, _} -> false
            {{:function, _name, _arity}, _, _, %{"en" => _doc}, _} -> true
            _ -> false
          end)
          |> Enum.group_by(fn {{:function, name, _arity}, _, _, _, _} -> name end)
          |> Enum.map(fn {name, functions} ->
            # Take the least arity version of each function
            case Enum.min_by(functions, fn {{:function, _name, arity}, _, _, _, _} -> arity end) do
              {{:function, _name, _arity}, _, _, %{"en" => doc}, _} ->
                [first_sentence | _] = String.split(doc, ".")
                first_sentence = String.replace(first_sentence, "\n", "")

                %{label: "#{inspect(module)}.#{name}", type: "function", info: first_sentence}
            end
          end)

        [module_completion | function_completions]

      _ ->
        []
    end
  end
end
