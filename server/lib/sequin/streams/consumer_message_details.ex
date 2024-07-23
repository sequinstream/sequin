defmodule Sequin.Streams.ConsumerMessageWithConsumerInfoss do
  @moduledoc false
  @derive {Jason.Encoder,
           only: [
             :consumer_id,
             :consumer_name,
             :consumer_filter_key_pattern,
             :state,
             :ack_id,
             :deliver_count,
             :last_delivered_at,
             :not_visible_until
           ]}
  @type state :: :acked | :available | :pending

  @type t :: %__MODULE__{
          consumer_id: Ecto.UUID.t(),
          consumer_name: String.t(),
          consumer_filter_key_pattern: String.t(),
          state: state(),
          ack_id: Ecto.UUID.t() | nil,
          deliver_count: non_neg_integer() | nil,
          last_delivered_at: DateTime.t() | nil,
          not_visible_until: DateTime.t() | nil
        }

  defstruct [
    :consumer_id,
    :consumer_name,
    :consumer_filter_key_pattern,
    :state,
    :ack_id,
    :deliver_count,
    :last_delivered_at,
    :not_visible_until
  ]
end
