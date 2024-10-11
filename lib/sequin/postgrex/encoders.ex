defmodule Sequin.Postgrex.Encoders do
  @moduledoc false

  defimpl Jason.Encoder, for: Postgrex.Lexeme do
    def encode(%Postgrex.Lexeme{} = lexeme, opts) do
      Jason.Encoder.encode(
        %{
          type: :lexeme,
          word: lexeme.word,
          positions: Enum.map(lexeme.positions, &Tuple.to_list/1)
        },
        opts
      )
    end
  end
end
