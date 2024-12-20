Mox.defmock(Sequin.Mocks.Extensions.MessageHandlerMock,
  for: Sequin.Extensions.MessageHandlerBehaviour
)

Mox.defmock(Sequin.Mocks.NatsMock,
  for: Sequin.Sinks.Nats
)

Mox.defmock(Sequin.Mocks.RabbitMqMock,
  for: Sequin.Sinks.RabbitMq
)
