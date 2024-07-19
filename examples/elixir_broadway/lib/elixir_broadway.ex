defmodule ExampleBroadway do
  use Broadway

  def start_link(opts) do
    stream = Keyword.get(opts, :stream, "default")
    consumer = Keyword.get(opts, :consumer)

    if not consumer, do: raise(ArgumentError, ":consumer is required")

    Broadway.start_link(ExampleBroadway,
      name: ExampleBroadway,
      producer: [
        module: {OffBroadwaySequin.Producer, [stream: stream, consumer: consumer]},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 2]
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    IO.inspect(message, label: "MESSAGE")
    message
  end
end
