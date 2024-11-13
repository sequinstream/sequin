defmodule Sequin.Aws.QueuePolicy do
  @moduledoc false
  use TypedStruct

  typedstruct do
    field :account_id, String.t(), enforce: true
    field :region, String.t(), enforce: true
    field :resource, String.t(), enforce: true
    field :statements, list(map()), default: []
  end

  def put_statement(%__MODULE__{} = qp, func) do
    Map.update!(qp, :statements, fn s ->
      [func.({qp.account_id, qp.region}) | s]
    end)
  end

  def to_map(%__MODULE__{} = qp) do
    %{
      "Statement" =>
        Enum.map(qp.statements, fn statement ->
          Map.put(statement, "Resource", qp.resource)
        end),
      "Version" => "2008-10-17"
    }
  end

  defimpl Jason.Encoder do
    alias Sequin.Aws.QueuePolicy

    def encode(%QueuePolicy{} = value, opts) do
      value
      |> QueuePolicy.to_map()
      |> Jason.Encode.map(opts)
    end
  end

  def new(queue_name, account_id, region) do
    put_statement(
      %__MODULE__{
        account_id: account_id,
        region: region,
        resource: "arn:aws:sqs:#{region}:#{account_id}:#{queue_name}"
      },
      fn {account_id, _} ->
        %{"Effect" => "Allow", "Principal" => %{"AWS" => "arn:aws:iam::#{account_id}:root"}, "Action" => "SQS:*"}
      end
    )
  end
end
