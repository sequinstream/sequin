Mox.defmock(Sequin.ConfigMock,
  for: Sequin.Config
)

Mox.defmock(Sequin.Runtime.MessageHandlerMock,
  for: Sequin.Runtime.MessageHandler
)

Mox.defmock(Sequin.Sinks.NatsMock,
  for: Sequin.Sinks.Nats
)

Mox.defmock(Sequin.Sinks.RabbitMqMock,
  for: Sequin.Sinks.RabbitMq
)

Mox.defmock(Sequin.Sinks.KafkaMock,
  for: Sequin.Sinks.Kafka
)

Mox.defmock(Sequin.Sinks.MysqlMock,
  for: Sequin.Sinks.Mysql
)

Mox.defmock(Sequin.Runtime.TableReaderServerMock,
  for: Sequin.Runtime.TableReaderServer
)

Mox.defmock(Sequin.TestSupport.DateTimeMock,
  for: Sequin.TestSupport.DateTime
)

Mox.defmock(Sequin.TestSupport.UUIDMock,
  for: Sequin.TestSupport.UUID
)

Mox.defmock(Sequin.AwsMock, for: Sequin.Aws)

Mox.defmock(Sequin.Runtime.SlotMessageStoreMock,
  for: Sequin.Runtime.SlotMessageStoreBehaviour
)

Mox.defmock(Sequin.TestSupport.EnumMock,
  for: Sequin.TestSupport.Enum
)

Mox.defmock(Sequin.TestSupport.ProcessMock,
  for: Sequin.TestSupport.Process
)

Mox.defmock(Sequin.Runtime.PageSizeOptimizerMock,
  for: Sequin.Runtime.PageSizeOptimizer
)

Mox.defmock(Sequin.Runtime.TableReaderMock,
  for: Sequin.Runtime.TableReader
)

Mox.defmock(Sequin.TestSupport.ApplicationMock,
  for: Sequin.ApplicationBehaviour
)

Mox.defmock(Sequin.Runtime.SinkPipelineMock,
  for: Sequin.Runtime.SinkPipeline
)
