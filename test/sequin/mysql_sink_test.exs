defmodule Sequin.Consumers.MysqlSinkTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.MysqlSink

  describe "changeset/2" do
    setup do
      %{
        valid_params: %{
          host: "localhost",
          port: 3306,
          database: "test_db",
          table_name: "test_table",
          username: "test_user",
          password: "test_password"
        }
      }
    end

    test "valid params have no errors", %{valid_params: params} do
      changeset = MysqlSink.changeset(%MysqlSink{}, params)
      assert Sequin.Error.errors_on(changeset) == %{}
    end

    test "validates required fields", %{valid_params: params} do
      changeset = MysqlSink.changeset(%MysqlSink{}, %{})
      errors = Sequin.Error.errors_on(changeset)

      assert errors[:host] == ["can't be blank"]
      assert errors[:database] == ["can't be blank"]
      assert errors[:table_name] == ["can't be blank"]
      assert errors[:username] == ["can't be blank"]
      assert errors[:password] == ["can't be blank"]
    end

    test "validates port range", %{valid_params: params} do
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | port: 0})
      assert Sequin.Error.errors_on(changeset)[:port] == ["must be greater than 0"]

      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | port: 70000})
      assert Sequin.Error.errors_on(changeset)[:port] == ["must be less than or equal to 65535"]
    end

    test "validates batch_size range", %{valid_params: params} do
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | batch_size: 0})
      assert Sequin.Error.errors_on(changeset)[:batch_size] == ["must be greater than 0"]

      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | batch_size: 20000})
      assert Sequin.Error.errors_on(changeset)[:batch_size] == ["must be less than or equal to 10000"]
    end

    test "validates timeout_seconds range", %{valid_params: params} do
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | timeout_seconds: 0})
      assert Sequin.Error.errors_on(changeset)[:timeout_seconds] == ["must be greater than 0"]

      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | timeout_seconds: 400})
      assert Sequin.Error.errors_on(changeset)[:timeout_seconds] == ["must be less than or equal to 300"]
    end

    test "validates table_name format", %{valid_params: params} do
      # Valid table names
      valid_names = ["table", "user_data", "Table123", "_private"]
      for name <- valid_names do
        changeset = MysqlSink.changeset(%MysqlSink{}, %{params | table_name: name})
        assert Sequin.Error.errors_on(changeset)[:table_name] == nil, "#{name} should be valid"
      end

      # Invalid table names
      invalid_names = ["123table", "table-name", "table space", ""]
      for name <- invalid_names do
        changeset = MysqlSink.changeset(%MysqlSink{}, %{params | table_name: name})
        assert Sequin.Error.errors_on(changeset)[:table_name] == ["must be a valid MySQL table name (alphanumeric and underscores, starting with letter or underscore)"], "#{name} should be invalid"
      end
    end

    test "validates string field lengths", %{valid_params: params} do
      # Test host length
      long_host = String.duplicate("a", 256)
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | host: long_host})
      assert Sequin.Error.errors_on(changeset)[:host] == ["should be at most 255 character(s)"]

      # Test database length
      long_database = String.duplicate("a", 65)
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | database: long_database})
      assert Sequin.Error.errors_on(changeset)[:database] == ["should be at most 64 character(s)"]

      # Test table_name length
      long_table = String.duplicate("a", 65)
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | table_name: long_table})
      assert Sequin.Error.errors_on(changeset)[:table_name] == ["should be at most 64 character(s)"]

      # Test username length
      long_username = String.duplicate("a", 33)
      changeset = MysqlSink.changeset(%MysqlSink{}, %{params | username: long_username})
      assert Sequin.Error.errors_on(changeset)[:username] == ["should be at most 32 character(s)"]
    end
  end

  describe "connection_opts/1" do
    test "generates correct connection options without SSL" do
      sink = %MysqlSink{
        host: "localhost",
        port: 3306,
        database: "test_db",
        username: "test_user",
        password: "test_password",
        ssl: false,
        timeout_seconds: 30
      }

      opts = MysqlSink.connection_opts(sink)

      expected = [
        hostname: "localhost",
        port: 3306,
        database: "test_db",
        username: "test_user",
        password: "test_password",
        timeout: 30_000,
        pool_size: 10
      ]

      assert opts == expected
    end

    test "generates correct connection options with SSL" do
      sink = %MysqlSink{
        host: "mysql.example.com",
        port: 3306,
        database: "prod_db",
        username: "prod_user",
        password: "prod_password",
        ssl: true,
        timeout_seconds: 60
      }

      opts = MysqlSink.connection_opts(sink)

      expected = [
        hostname: "mysql.example.com",
        port: 3306,
        database: "prod_db",
        username: "prod_user",
        password: "prod_password",
        timeout: 60_000,
        pool_size: 10,
        ssl: true
      ]

      assert opts == expected
    end
  end
end
