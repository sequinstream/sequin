Mox.defmock(Sequin.Mocks.DatabasesRuntime.MessageHandlerMock,
  for: Sequin.DatabasesRuntime.SlotProcessor.MessageHandlerBehaviour
)

Mox.defmock(Sequin.Mocks.NatsMock,
  for: Sequin.Sinks.Nats
)

Mox.defmock(Sequin.Mocks.RabbitMqMock,
  for: Sequin.Sinks.RabbitMq
)

Mox.defmock(Sequin.Mocks.TableReaderServerMock,
  for: Sequin.DatabasesRuntime.TableReaderServer
)

Mox.defmock(Sequin.TestSupport.DateTimeMock,
  for: Sequin.TestSupport.DateTime
)
