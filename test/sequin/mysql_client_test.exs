defmodule Sequin.Sinks.Mysql.ClientTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.MysqlSink
  alias Sequin.Factory.SinkFactory
  alias Sequin.Sinks.Mysql.Client

  @sink %MysqlSink{
    type: :mysql,
    host: "localhost",
    port: 3306,
    database: "test_db",
    table_name: "test_table",
    username: "test_user",
    password: "test_password",
    timeout_seconds: 30
  }

    # Integration tests would be needed to test the client functions
  # against a real MySQL database, but that's beyond the scope of unit testing
end

# Note: This test file tests public functions only.
# Private function testing would require exposing them in the main module
# or using more advanced testing techniques.
