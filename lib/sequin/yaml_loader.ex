defmodule Sequin.YamlLoader do
  @moduledoc false
  alias Sequin.Accounts
  alias Sequin.Accounts.Account
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.WebhookSiteGenerator
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Replication
  alias Sequin.Repo

  require Logger

  @app :sequin

  def apply! do
    Logger.info("Applying config")
    load_app()
    ensure_repo_started!()

    cond do
      not self_hosted?() ->
        Logger.info("Not self-hosted, skipping config loading")

      not is_nil(config_file_yaml()) ->
        Logger.info("Loading from config file YAML")

        config_file_yaml()
        |> Base.decode64!()
        |> apply_from_yml!()

      not is_nil(config_file_path()) ->
        Logger.info("Loading from config file path")

        config_file_path()
        |> File.read!()
        |> apply_from_yml!()

      true ->
        Logger.info("No config file YAML or path, skipping config loading")
    end

    :ok
  end

  def apply_from_yml!(yml) do
    case apply_from_yml(yml) do
      {:ok, {:ok, _resources}} -> :ok
      {:ok, {:error, error}} -> raise "Failed to apply config: #{inspect(error)}"
      {:error, error} -> raise "Failed to apply config: #{inspect(error)}"
    end
  end

  def apply_from_yml(account_id \\ nil, yml) do
    case YamlElixir.read_from_string(yml) do
      {:ok, config} ->
        Repo.transaction(fn ->
          case apply_config(account_id, config) do
            {:ok, resources} -> {:ok, resources}
            {:error, error} -> Repo.rollback(error)
          end
        end)

      {:error, %YamlElixir.ParsingError{} = error} ->
        {:error, Error.bad_request(message: "Invalid YAML: #{Exception.message(error)}")}

      {:error, error} ->
        Logger.error("Error reading config file: #{inspect(error)}")
        {:error, error}
    end
  end

  def plan_from_yml(account_id \\ nil, yml) do
    ## return a list of changesets
    case YamlElixir.read_from_string(yml) do
      {:ok, config} ->
        result =
          Repo.transaction(fn ->
            account_id
            |> apply_config(config)
            |> Repo.rollback()
          end)

        case result do
          {:error, {:ok, planned_resources}} ->
            # Get the account id from the planned resources if it exists
            # account_id is nil if the account is not found, ie. it's a new account
            account_id =
              planned_resources
              |> Sequin.Enum.find!(&is_struct(&1, Account))
              |> Map.fetch!(:id)
              |> Accounts.get_account()
              |> case do
                {:ok, account} -> account.id
                {:error, %NotFoundError{}} -> nil
              end

            current_resources = all_resources(account_id)
            {:ok, planned_resources, current_resources}

          {:error, {:error, error}} ->
            {:error, error}
        end

      {:error, error} ->
        Logger.error("Error reading config file: #{inspect(error)}")
        {:error, error}
    end
  end

  defp apply_config(account_id, config) do
    with {:ok, account} <- find_or_create_account(account_id, config),
         {:ok, _users} <- find_or_create_users(account, config),
         {:ok, _databases} <- upsert_databases(account.id, config),
         databases = Databases.list_dbs_for_account(account.id),
         {:ok, _sequences} <- find_or_create_sequences(account.id, config, databases),
         databases = Databases.list_dbs_for_account(account.id, [:sequences, :replication_slot]),
         {:ok, _wal_pipelines} <- upsert_wal_pipelines(account.id, config, databases),
         {:ok, _http_endpoints} <- upsert_http_endpoints(account.id, config),
         http_endpoints = Consumers.list_http_endpoints_for_account(account.id),
         {:ok, _sink_consumers} <- upsert_sink_consumers(account.id, config, databases, http_endpoints) do
      {:ok, all_resources(account.id)}
    end
  end

  def all_resources(nil), do: []

  def all_resources(account_id) do
    account = Accounts.get_account!(account_id)
    users = Accounts.list_users_for_account(account_id)
    databases = Databases.list_dbs_for_account(account_id, [:sequences, :replication_slot])
    wal_pipelines = Replication.list_wal_pipelines_for_account(account_id, [:source_database, :destination_database])
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)

    sink_consumers =
      account_id
      |> Consumers.list_sink_consumers_for_account(sequence: [:postgres_database])
      |> Enum.map(&SinkConsumer.preload_http_endpoint/1)

    [account | users] ++ databases ++ wal_pipelines ++ http_endpoints ++ sink_consumers
  end

  #############
  ## Account ##
  #############

  # account_id is nil here if we are loading directly from the config file
  # if the yml is passed in from the API, we expect the account_id to be passed in as well
  defp find_or_create_account(nil, config) do
    if self_hosted?() do
      do_find_or_create_account(config)
    else
      {:error, Error.unauthorized(message: "account configuration is not supported in Sequin Cloud")}
    end
  end

  defp find_or_create_account(account_id, %{"account" => _}) when not is_nil(account_id) do
    {:error,
     Error.bad_request(message: "Account configuration only supported when self hosting and using a config file on boot.")}
  end

  defp find_or_create_account(account_id, _config) when not is_nil(account_id) do
    Accounts.get_account(account_id)
  end

  defp do_find_or_create_account(%{"account" => %{"name" => name}}) do
    case Accounts.find_account(name: name) do
      {:ok, account} ->
        Logger.info("Found account: #{inspect(account, pretty: true)}")
        {:ok, account}

      {:error, %NotFoundError{}} ->
        case Accounts.create_account(%{name: name}) do
          {:ok, account} ->
            Logger.info("Created account: #{inspect(account, pretty: true)}")
            {:ok, account}

          {:error, error} ->
            {:error, Error.bad_request(message: "Error creating account '#{name}': #{Exception.message(error)}")}
        end
    end
  end

  defp do_find_or_create_account(%{}) do
    {:error, Error.bad_request(message: "Account configuration is required.")}
  end

  ###########
  ## Users ##
  ###########

  defp find_or_create_users(account, %{"users" => users}) do
    Logger.info("Creating users: #{inspect(users, pretty: true)}")

    Enum.reduce_while(users, {:ok, []}, fn user_attrs, {:ok, acc} ->
      case find_or_create_user(account, user_attrs) do
        {:ok, user} -> {:cont, {:ok, [user | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp find_or_create_users(_account, %{}) do
    Logger.info("No users found in config")
    {:ok, []}
  end

  defp find_or_create_user(account, %{"email" => email} = user_attrs) do
    case Accounts.get_user_by_email(:identity, email) do
      nil -> create_user(account, user_attrs)
      user -> {:ok, user}
    end
  end

  defp create_user(account, %{"email" => email, "password" => password}) do
    user_params = %{
      email: email,
      password: password,
      password_confirmation: password
    }

    Accounts.register_user(:identity, user_params, account)
  end

  ###############
  ## Databases ##
  ###############

  @database_defaults %{
    "username" => "postgres",
    "password" => "postgres",
    "port" => 5432
  }

  defp upsert_databases(account_id, %{"databases" => databases}) do
    Logger.info("Upserting databases: #{inspect(databases, pretty: true)}")

    Enum.reduce_while(databases, {:ok, []}, fn database_attrs, {:ok, acc} ->
      database_attrs = Map.delete(database_attrs, "tables")

      case upsert_database(account_id, database_attrs) do
        {:ok, database} -> {:cont, {:ok, [database | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_databases(_account_id, %{}) do
    Logger.info("No databases found in config")
    {:ok, []}
  end

  defp upsert_database(account_id, %{"name" => name} = database_attrs) do
    account_id
    |> Databases.get_db_for_account(name)
    |> case do
      {:ok, database} ->
        Logger.info("Found database: #{inspect(database, pretty: true)}")
        update_database(database, database_attrs)

      {:error, %NotFoundError{}} ->
        create_database_with_replication(account_id, database_attrs)
    end
  end

  defp create_database_with_replication(account_id, database) do
    database = Map.merge(@database_defaults, database)

    account_id
    |> Databases.create_db(database)
    |> case do
      {:ok, db} ->
        replication_params = Map.put(database, "postgres_database_id", db.id)

        case Replication.create_pg_replication(account_id, replication_params) do
          {:ok, replication} ->
            Logger.info("Created database: #{inspect(db, pretty: true)}")
            {:ok, %PostgresDatabase{db | replication_slot: replication}}

          {:error, error} when is_exception(error) ->
            {:error,
             Error.bad_request(
               message: "Error creating replication slot for database '#{db.name}': #{Exception.message(error)}"
             )}

          {:error, %Ecto.Changeset{} = changeset} ->
            {:error,
             Error.bad_request(
               message: "Error creating replication slot for database '#{db.name}': #{inspect(changeset, pretty: true)}"
             )}
        end

      {:error, %DBConnection.ConnectionError{}} ->
        {:error,
         Error.bad_request(
           message: "Failed to connect to database '#{database["name"]}'. Please check your database credentials."
         )}

      {:error, error} when is_exception(error) ->
        {:error, Error.bad_request(message: "Error creating database '#{database["name"]}': #{Exception.message(error)}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(message: "Error creating database '#{database["name"]}': #{inspect(changeset, pretty: true)}")}
    end
  end

  defp update_database(database, attrs) do
    case Databases.update_db(database, attrs) do
      {:ok, database} ->
        Logger.info("Updated database: #{inspect(database, pretty: true)}")
        {:ok, database}

      {:error, error} when is_exception(error) ->
        {:error, Error.bad_request(message: "Error updating database '#{database.name}': #{Exception.message(error)}")}
    end
  end

  ###############
  ## Sequences ##
  ###############

  defp find_or_create_sequences(account_id, %{"databases" => databases}, db_records) do
    Logger.info("Creating sequences from database tables")

    # Flatten all tables from all databases into sequence configs
    sequence_configs =
      Enum.flat_map(databases, fn database ->
        tables = database["tables"] || []

        Enum.map(tables, fn table ->
          database_name = database["name"]
          table_name = table["table_name"]
          schema = table["table_schema"] || "public"
          name = "#{database_name}.#{schema}.#{table_name}"

          %{
            "name" => name,
            "database" => database_name,
            "table_schema" => schema,
            "table_name" => table_name,
            "sort_column_name" => table["sort_column_name"]
          }
        end)
      end)

    Enum.reduce_while(sequence_configs, {:ok, []}, fn sequence, {:ok, acc} ->
      database = database_for_sequence!(sequence, db_records)

      case find_or_create_sequence(account_id, database, sequence) do
        {:ok, sequence} -> {:cont, {:ok, [sequence | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp find_or_create_sequences(_account_id, _config, _databases) do
    Logger.info("No database tables found in config")
    {:ok, []}
  end

  defp database_for_sequence!(%{"database" => database_name}, databases) do
    Sequin.Enum.find!(databases, fn database -> database.name == database_name end)
  end

  defp find_or_create_sequence(account_id, %PostgresDatabase{} = database, sequence_attrs) do
    case Databases.find_sequence_for_account(account_id, name: sequence_attrs["name"]) do
      {:ok, sequence} ->
        Logger.info("Found sequence: #{inspect(sequence, pretty: true)}")
        {:ok, sequence}

      {:error, %NotFoundError{}} ->
        upsert_sequence(account_id, database, sequence_attrs)
    end
  end

  defp upsert_sequence(account_id, %PostgresDatabase{id: id} = database, sequence) do
    with {:ok, table} <- table_for_sequence(database, sequence),
         {:ok, sort_column_attnum} <- sort_column_attnum_for_sequence(table, sequence) do
      attrs =
        sequence
        |> Map.put("postgres_database_id", id)
        |> Map.put("table_oid", table.oid)
        |> Map.put("sort_column_attnum", sort_column_attnum)

      account_id
      |> Databases.upsert_sequence(attrs)
      |> case do
        {:ok, sequence} ->
          Logger.info("Created stream: #{inspect(sequence, pretty: true)}")
          {:ok, sequence}

        {:error, error} when is_exception(error) ->
          {:error, Error.bad_request(message: "Error creating stream '#{sequence["name"]}': #{Exception.message(error)}")}

        {:error, %Ecto.Changeset{} = changeset} ->
          {:error,
           Error.bad_request(message: "Error creating stream '#{sequence["name"]}': #{inspect(changeset, pretty: true)}")}
      end
    end
  end

  defp table_for_sequence(database, %{"table_schema" => table_schema, "table_name" => table_name}) do
    case Enum.find(database.tables, fn table -> table.name == table_name and table.schema == table_schema end) do
      nil -> {:error, Error.not_found(entity: :table, params: %{table_schema: table_schema, table_name: table_name})}
      table -> {:ok, table}
    end
  end

  defp table_for_sequence(_, %{}) do
    {:error, Error.bad_request(message: "`table` and `schema` are required for each stream")}
  end

  defp sort_column_attnum_for_sequence(table, %{"sort_column_name" => sort_column_name}) do
    case Enum.find(table.columns, fn column -> column.name == sort_column_name end) do
      nil ->
        {:error,
         Error.not_found(
           entity: :column,
           params: %{table_schema: table.schema, table_name: table.name, column_name: sort_column_name}
         )}

      column ->
        {:ok, column.attnum}
    end
  end

  defp sort_column_attnum_for_sequence(_, %{}) do
    {:ok, nil}
  end

  ##############################
  ## Change Retention         ##
  ##############################

  defp upsert_wal_pipelines(account_id, %{"change_retentions" => wal_pipelines}, databases) do
    Logger.info("Setting up change retention: #{inspect(wal_pipelines, pretty: true)}")

    Enum.reduce_while(wal_pipelines, {:ok, []}, fn wal_pipeline, {:ok, acc} ->
      case upsert_wal_pipeline(account_id, wal_pipeline, databases) do
        {:ok, wal_pipeline} -> {:cont, {:ok, [wal_pipeline | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_wal_pipelines(_account_id, %{}, _databases) do
    Logger.info("Change retention not setup in config")
    {:ok, []}
  end

  defp upsert_wal_pipeline(account_id, %{"name" => name} = attrs, databases) do
    case Replication.find_wal_pipeline_for_account(account_id, name: name) do
      {:ok, wal_pipeline} ->
        update_wal_pipeline(wal_pipeline, attrs, databases)

      {:error, %NotFoundError{}} ->
        create_wal_pipeline(account_id, attrs, databases)
    end
  end

  defp upsert_wal_pipeline(_account_id, %{}, _databases) do
    {:error, Error.bad_request(message: "`name` is required to setup change retention")}
  end

  defp create_wal_pipeline(account_id, attrs, databases) do
    params = parse_wal_pipeline_attrs(attrs, databases)

    account_id
    |> Replication.create_wal_pipeline_with_lifecycle(params)
    |> case do
      {:ok, wal_pipeline} ->
        {:ok, wal_pipeline}

      {:error, error} when is_exception(error) ->
        {:error,
         Error.bad_request(message: "Error setting up change retention '#{attrs["name"]}': #{Exception.message(error)}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(
           message: "Error setting up change retention '#{attrs["name"]}': #{inspect(changeset, pretty: true)}"
         )}
    end
  end

  defp update_wal_pipeline(wal_pipeline, attrs, databases) do
    params = parse_wal_pipeline_attrs(attrs, databases)

    case Replication.update_wal_pipeline_with_lifecycle(wal_pipeline, params) do
      {:ok, wal_pipeline} ->
        Logger.info("Updated change retention: #{inspect(wal_pipeline, pretty: true)}")
        {:ok, wal_pipeline}

      {:error, error} when is_exception(error) ->
        {:error,
         Error.bad_request(message: "Error updating change retention '#{wal_pipeline.name}': #{Exception.message(error)}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(
           message: "Error updating change retention '#{wal_pipeline.name}': #{inspect(changeset, pretty: true)}"
         )}
    end
  end

  defp parse_wal_pipeline_attrs(
         %{
           "name" => name,
           "source_database" => source_db_name,
           "source_table_schema" => source_schema,
           "source_table_name" => source_table,
           "destination_database" => dest_db_name,
           "destination_table_schema" => dest_schema,
           "destination_table_name" => dest_table
         } = attrs,
         databases
       ) do
    # Find source database
    source_database = Enum.find(databases, &(&1.name == source_db_name))
    unless source_database, do: raise("Source database '#{source_db_name}' not found")

    # Find destination database
    destination_database = Enum.find(databases, &(&1.name == dest_db_name))
    unless destination_database, do: raise("Destination database '#{dest_db_name}' not found")

    # Find destination table
    destination_table =
      Enum.find(destination_database.tables, fn table ->
        table.schema == dest_schema && table.name == dest_table
      end)

    unless destination_table, do: raise("Destination table '#{dest_schema}.#{dest_table}' not found")

    # Find source table
    source_table_struct =
      Enum.find(source_database.tables, fn t ->
        t.schema == source_schema && t.name == source_table
      end)

    unless source_table_struct, do: raise("Table '#{source_schema}.#{source_table}' not found")

    # Build source_tables config
    source_table_config = %{
      "schema_name" => source_schema,
      "table_name" => source_table,
      "oid" => source_table_struct.oid,
      # Default to all actions
      "actions" => attrs["actions"] || [:insert, :update, :delete],
      "column_filters" => parse_column_filters(attrs["filters"], source_database, source_schema, source_table)
    }

    %{
      name: name,
      status: parse_status(attrs["status"]),
      replication_slot_id: source_database.replication_slot.id,
      destination_database_id: destination_database.id,
      destination_oid: destination_table.oid,
      source_tables: [source_table_config]
    }
  end

  # Helper to parse column filters for WAL pipeline
  defp parse_column_filters(nil, _database, _schema, _table), do: []

  defp parse_column_filters(filters, database, schema, table_name) when is_list(filters) do
    # Find the table
    table =
      Enum.find(database.tables, fn t ->
        t.schema == schema && t.name == table_name
      end)

    unless table, do: raise("Table '#{schema}.#{table_name}' not found")

    # Parse each filter
    Enum.map(filters, &parse_column_filter(&1, table))
  end

  ####################
  ## HTTP Endpoints ##
  ####################

  @http_endpoint_docs """
  HTTP Endpoints are destinations for Webhook Subscriptions.

  They can be configured in one of three ways:

  1. Configure an internet accessible URL:

  http_endpoints:
    - name: "external-endpoint"
      url: "https://api.example.com/webhook"

  2. Configure a local endpoint that uses the Sequin CLI to create a secure tunnel:

  http_endpoints:
    - name: "local-endpoint"
      local: "true"
      path: "/webhook"

  3. Configure a Webhook.site endpoint for development purposes:

  http_endpoints:
    - name: "webhook.site-endpoint"
      webhook.site: "true"


  Shared options:

  - name: "some-name"
  - headers:
      - key: "X-Header"
        value: "my-value"
  - encrypted_headers:
      - key: "X-Secret-Header"
        value: "super-secret"
  """

  defp upsert_http_endpoints(account_id, %{"http_endpoints" => http_endpoints}) do
    Logger.info("Creating HTTP endpoints: #{inspect(http_endpoints, pretty: true)}")

    Enum.reduce_while(http_endpoints, {:ok, []}, fn http_endpoint, {:ok, acc} ->
      case upsert_http_endpoint(account_id, http_endpoint) do
        {:ok, http_endpoint} ->
          {:cont, {:ok, [http_endpoint | acc]}}

        {:error, error} ->
          {:halt,
           {:error, Error.bad_request(message: "Invalid HTTP endpoint configuration: #{Exception.message(error)}")}}
      end
    end)
  end

  defp upsert_http_endpoints(_account_id, %{}) do
    Logger.info("No HTTP endpoints found in config")
    {:ok, []}
  end

  defp upsert_http_endpoint(account_id, %{"name" => name} = attrs) do
    case Sequin.Consumers.find_http_endpoint_for_account(account_id, name: name) do
      {:ok, endpoint} ->
        update_http_endpoint(endpoint, attrs)

      {:error, %NotFoundError{}} ->
        create_http_endpoint(account_id, attrs)
    end
  end

  defp create_http_endpoint(account_id, attrs) do
    with {:ok, endpoint_params} <- parse_http_endpoint_attrs(attrs),
         {:ok, endpoint} <- Sequin.Consumers.create_http_endpoint(account_id, endpoint_params) do
      Logger.info("Created HTTP endpoint: #{inspect(endpoint, pretty: true)}")
      {:ok, endpoint}
    else
      {:error, error} ->
        {:error,
         Error.bad_request(message: "Error creating HTTP endpoint '#{attrs["name"]}': #{Exception.message(error)}")}
    end
  end

  defp update_http_endpoint(endpoint, attrs) do
    with {:ok, endpoint_params} <- parse_http_endpoint_attrs(attrs),
         {:ok, endpoint} <- Sequin.Consumers.update_http_endpoint(endpoint, endpoint_params) do
      Logger.info("Updated HTTP endpoint: #{inspect(endpoint, pretty: true)}")
      {:ok, endpoint}
    else
      {:error, error} ->
        {:error,
         Error.bad_request(message: "Error updating HTTP endpoint '#{endpoint.name}': #{Exception.message(error)}")}
    end
  end

  defp parse_http_endpoint_attrs(%{"name" => name} = attrs) do
    case attrs do
      # Webhook.site endpoint
      %{"webhook.site" => t} when t in [true, "true"] ->
        {:ok,
         %{
           name: name,
           scheme: :https,
           host: "webhook.site",
           path: "/" <> generate_webhook_site_id()
         }}

      # Local endpoint
      %{"local" => "true"} = local_attrs ->
        with {:ok, headers} <- parse_headers(local_attrs["headers"]),
             {:ok, encrypted_headers} <- parse_headers(local_attrs["encrypted_headers"]) do
          {:ok,
           %{
             name: name,
             use_local_tunnel: true,
             path: local_attrs["path"],
             headers: headers,
             encrypted_headers: encrypted_headers
           }}
        end

      # External endpoint with URL
      %{"url" => url} = external_attrs ->
        uri = URI.parse(url)

        with {:ok, headers} <- parse_headers(external_attrs["headers"]),
             {:ok, encrypted_headers} <- parse_headers(external_attrs["encrypted_headers"]) do
          {:ok,
           %{
             name: name,
             scheme: String.to_existing_atom(uri.scheme),
             host: uri.host,
             port: uri.port,
             path: uri.path,
             query: uri.query,
             fragment: uri.fragment,
             headers: headers,
             encrypted_headers: encrypted_headers
           }}
        end

      _ ->
        {:error, Error.validation(summary: "Invalid HTTP endpoint configuration for '#{name}'\n\n#{@http_endpoint_docs}")}
    end
  end

  # Helper functions

  defp parse_headers(nil), do: {:ok, %{}}

  defp parse_headers(headers) when headers == %{} do
    {:ok, %{}}
  end

  defp parse_headers(headers) when is_list(headers) do
    {:ok, Map.new(headers, fn %{"key" => key, "value" => value} -> {key, value} end)}
  end

  defp parse_headers(headers) when is_binary(headers) do
    {:error, Error.validation(summary: "Invalid headers configuration. Must be a list of key-value pairs.")}
  end

  defp generate_webhook_site_id do
    if env() == :test do
      UUID.uuid4()
    else
      case WebhookSiteGenerator.generate() do
        {:ok, uuid} ->
          uuid

        {:error, reason} ->
          raise "Failed to create webhook.site endpoint: #{reason}"
      end
    end
  end

  #########################
  ## HTTP Push Consumers ##
  #########################

  defp upsert_sink_consumers(account_id, %{"sinks" => consumers}, databases, http_endpoints) do
    Logger.info("Upserting HTTP push consumers: #{inspect(consumers, pretty: true)}")

    Enum.reduce_while(consumers, {:ok, []}, fn consumer, {:ok, acc} ->
      case upsert_sink_consumer(account_id, consumer, databases, http_endpoints) do
        {:ok, consumer} -> {:cont, {:ok, [consumer | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_sink_consumers(_account_id, %{}, _databases, _http_endpoints), do: {:ok, []}

  defp upsert_sink_consumer(account_id, %{"name" => name} = consumer_attrs, databases, http_endpoints) do
    # Find existing consumer first
    case Sequin.Consumers.find_sink_consumer(account_id, name: name) do
      {:ok, existing_consumer} ->
        with {:ok, params} <- parse_sink_consumer_params(consumer_attrs, databases, http_endpoints),
             {:ok, consumer} <- Sequin.Consumers.update_sink_consumer(existing_consumer, params) do
          Logger.info("Updated HTTP push consumer: #{inspect(consumer, pretty: true)}")
          {:ok, consumer}
        end

      {:error, %NotFoundError{}} ->
        with {:ok, params} <- parse_sink_consumer_params(consumer_attrs, databases, http_endpoints),
             {:ok, consumer} <-
               Sequin.Consumers.create_sink_consumer(account_id, params) do
          Logger.info("Created HTTP push consumer: #{inspect(consumer, pretty: true)}")
          {:ok, consumer}
        end
    end
  end

  defp parse_sink_consumer_params(
         %{"name" => name, "database" => database_name, "table" => table_ref, "destination" => sink_attrs} =
           consumer_attrs,
         databases,
         http_endpoints
       ) do
    # Split table reference into schema and name
    {schema, table_name} = parse_table_reference(table_ref)

    # Find the database and sequence
    with {:ok, database} <- find_database_by_name(database_name, databases),
         sequence_name = "#{database_name}.#{schema}.#{table_name}",
         {:ok, sequence} <- find_sequence_by_name(databases, sequence_name),
         {:ok, sink} <- parse_sink(sink_attrs, %{http_endpoints: http_endpoints}) do
      table = Sequin.Enum.find!(database.tables, &(&1.schema == schema && &1.name == table_name))

      {:ok,
       %{
         name: name,
         status: parse_status(consumer_attrs["status"]),
         sequence_id: sequence.id,
         replication_slot_id: database.replication_slot.id,
         sequence_filter: %{
           actions: ["insert", "update", "delete"],
           group_column_attnums: group_column_attnums(consumer_attrs["group_column_names"], table),
           column_filters: column_filters(consumer_attrs["filters"], table)
         },
         batch_size: Map.get(consumer_attrs, "batch_size", 1),
         sink: sink
       }}
    end
  end

  defp parse_sink(nil, _resources), do: {:error, Error.validation(summary: "`sink` is required on sink consumers.")}

  defp parse_sink(%{"type" => "sequin_stream"}, _resources) do
    {:ok, %{type: :sequin_stream}}
  end

  defp parse_sink(%{"type" => "webhook"} = attrs, resources) do
    http_endpoints = resources.http_endpoints

    with {:ok, http_endpoint} <- find_http_endpoint_by_name(http_endpoints, attrs["http_endpoint"]) do
      {:ok,
       %{
         type: :http_push,
         http_endpoint_id: http_endpoint.id,
         http_endpoint_path: attrs["http_endpoint_path"]
       }}
    end
  end

  defp parse_sink(%{"type" => "kafka"} = attrs, _resources) do
    with {:ok, sasl_mechanism} <- parse_sasl_mechanism(attrs["sasl_mechanism"]) do
      {:ok,
       %{
         type: :kafka,
         hosts: attrs["hosts"],
         topic: attrs["topic"],
         tls: attrs["tls"] || false,
         username: attrs["username"],
         password: attrs["password"],
         sasl_mechanism: sasl_mechanism
       }}
    end
  end

  defp parse_sink(%{"type" => "sqs"} = attrs, _resources) do
    {:ok,
     %{
       type: :sqs,
       queue_url: attrs["queue_url"],
       region: attrs["region"],
       access_key_id: attrs["access_key_id"],
       secret_access_key: attrs["secret_access_key"]
     }}
  end

  defp parse_sink(%{"type" => "redis"} = attrs, _resources) do
    {:ok,
     %{
       type: :redis,
       host: attrs["host"],
       port: attrs["port"],
       stream_key: attrs["stream_key"],
       database: attrs["database"] || 0,
       tls: attrs["tls"] || false,
       username: attrs["username"],
       password: attrs["password"]
     }}
  end

  defp parse_sink(%{"type" => "azure_event_hub"} = attrs, _resources) do
    {:ok,
     %{
       type: :azure_event_hub,
       namespace: attrs["namespace"],
       event_hub_name: attrs["event_hub_name"],
       shared_access_key_name: attrs["shared_access_key_name"],
       shared_access_key: attrs["shared_access_key"]
     }}
  end

  defp parse_sink(%{"type" => "nats"} = attrs, _resources) do
    {:ok,
     %{
       type: :nats,
       host: attrs["host"],
       port: attrs["port"],
       username: attrs["username"],
       password: attrs["password"],
       jwt: attrs["jwt"],
       nkey_seed: attrs["nkey_seed"],
       tls: attrs["tls"] || false
     }}
  end

  defp parse_sink(%{"type" => "gcp_pubsub"} = attrs, _resources) do
    {:ok,
     %{
       type: :gcp_pubsub,
       project_id: attrs["project_id"],
       topic_id: attrs["topic_id"],
       credentials: attrs["credentials"]
     }}
  end

  defp find_database_by_name(name, databases) do
    case Enum.find(databases, &(&1.name == name)) do
      nil -> {:error, Error.not_found(entity: :database, params: %{name: name})}
      database -> {:ok, database}
    end
  end

  defp find_http_endpoint_by_name(http_endpoints, name) do
    case Enum.find(http_endpoints, &(&1.name == name)) do
      nil -> {:error, Error.not_found(entity: :http_endpoint, params: %{name: name})}
      http_endpoint -> {:ok, http_endpoint}
    end
  end

  # Helper to parse table reference into schema and name
  defp parse_table_reference(table_ref) do
    case String.split(table_ref, ".", parts: 2) do
      [table_name] -> {"public", table_name}
      [schema, table_name] -> {schema, table_name}
    end
  end

  defp find_sequence_by_name(databases, sequence_name) do
    databases
    |> Enum.find_value(fn database ->
      Enum.find(database.sequences, &(&1.name == sequence_name))
    end)
    |> case do
      nil -> {:error, Error.not_found(entity: :sequence, params: %{name: sequence_name})}
      sequence -> {:ok, sequence}
    end
  end

  defp group_column_attnums(nil, %PostgresDatabaseTable{} = table) do
    PostgresDatabaseTable.default_group_column_attnums(table)
  end

  defp group_column_attnums(column_names, table) when is_list(column_names) do
    table.columns
    |> Enum.filter(&(&1.name in column_names))
    |> Enum.map(& &1.attnum)
  end

  defp parse_status(nil), do: :active
  defp parse_status("active"), do: :active
  defp parse_status("disabled"), do: :disabled
  defp parse_status("paused"), do: :paused

  defp parse_status(invalid_status) do
    raise "Invalid status '#{invalid_status}' for sink. Must be one of 'active', 'disabled', or 'paused'"
  end

  defp column_filters(nil, _table), do: []

  defp column_filters(filters, table) when is_list(filters) do
    Enum.map(filters, &parse_column_filter(&1, table))
  end

  defp parse_column_filter(%{"column_name" => column_name} = filter, table) do
    # Find the column by name
    column = Enum.find(table.columns, &(&1.name == column_name))
    unless column, do: raise("Column '#{column_name}' not found in table '#{table.name}'")

    is_jsonb = filter["field_path"] != nil
    value_type = determine_value_type(filter, column)

    Sequin.Consumers.SequenceFilter.ColumnFilter.from_external(%{
      "columnAttnum" => column.attnum,
      "operator" => filter["operator"],
      "valueType" => value_type,
      "value" => filter["comparison_value"],
      "isJsonb" => is_jsonb,
      "jsonbPath" => filter["field_path"]
    })
  end

  defp determine_value_type(%{"field_type" => explicit_type}, _column) when not is_nil(explicit_type) do
    case String.downcase(explicit_type) do
      "string" -> :string
      "cistring" -> :cistring
      "number" -> :number
      "boolean" -> :boolean
      "datetime" -> :datetime
      "list" -> :list
      invalid_type -> raise "Invalid field_type: #{invalid_type}"
    end
  end

  defp determine_value_type(%{"operator" => operator}, _column) when operator in ["is null", "is not null", "not null"] do
    :null
  end

  defp determine_value_type(%{"operator" => operator}, _column) when operator in ["in", "not in"] do
    :list
  end

  defp determine_value_type(_filter, column) do
    case column.type do
      "character varying" -> :string
      "text" -> :string
      "citext" -> :cistring
      "integer" -> :number
      "bigint" -> :number
      "numeric" -> :number
      "double precision" -> :number
      "boolean" -> :boolean
      "timestamp without time zone" -> :datetime
      "timestamp with time zone" -> :datetime
      "jsonb" -> :string
      "json" -> :string
      type -> raise "Unsupported column type: #{type}"
    end
  end

  ###############
  ## Utilities ##
  ###############

  defp config_file_yaml do
    Application.get_env(@app, :config_file_yaml)
  end

  defp config_file_path do
    Application.get_env(@app, :config_file_path)
  end

  defp load_app do
    Application.load(@app)
  end

  defp ensure_repo_started! do
    Application.ensure_all_started(:sequin)
  end

  defp self_hosted? do
    Application.get_env(@app, :self_hosted, false)
  end

  defp env do
    Application.fetch_env!(@app, :env)
  end

  # Helper function to parse SASL mechanism
  defp parse_sasl_mechanism(nil), do: {:ok, nil}
  defp parse_sasl_mechanism("plain"), do: {:ok, :plain}
  defp parse_sasl_mechanism("scram_sha_256"), do: {:ok, :scram_sha_256}
  defp parse_sasl_mechanism("scram_sha_512"), do: {:ok, :scram_sha_512}

  defp parse_sasl_mechanism(invalid),
    do:
      {:error,
       Error.validation(
         summary: "Invalid SASL mechanism '#{invalid}'. Must be one of: plain, scram_sha_256, scram_sha_512"
       )}
end
