Mox.defmock(Sequin.Mocks.DatabasesRuntime.MessageHandlerMock,
  for: Sequin.DatabasesRuntime.SlotProcessor.MessageHandlerBehaviour
)

Mox.defmock(Sequin.Mocks.NatsMock,
  for: Sequin.Sinks.Nats
)

Mox.defmock(Sequin.Mocks.RabbitMqMock,
  for: Sequin.Sinks.RabbitMq
)
