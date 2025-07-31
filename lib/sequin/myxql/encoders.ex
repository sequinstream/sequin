defimpl Jason.Encoder, for: MyXQL.Error do
  def encode(%MyXQL.Error{} = error, opts) do
    Jason.Encoder.encode(
      %{
        type: :mysql_error,
        message: error.message,
        connection_id: error.connection_id,
        mysql: error.mysql,
        statement: error.statement
      },
      opts
    )
  end
end
