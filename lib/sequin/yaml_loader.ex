defmodule Sequin.YamlLoader do
  @moduledoc false
  alias Sequin.Accounts
  alias Sequin.Accounts.Account
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Transforms

  require Logger

  @app :sequin

  def config_file_path do
    Application.get_env(@app, :config_file_path)
  end

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
      {:ok, {:ok, _resources}} ->
        :ok

      # Hack to give a better error message when table is missing
      {:ok, {:error, %NotFoundError{entity: :sequence} = error}} ->
        error = %{error | entity: :postgres_table}
        raise "Failed to apply config: #{inspect(error)}"

      {:ok, {:error, error}} ->
        raise "Failed to apply config: #{inspect(error)}"

      {:error, error} ->
        raise "Failed to apply config: #{inspect(error)}"
    end
  end

  def apply_from_yml(account_id \\ nil, yml, opts \\ []) do
    case YamlElixir.read_from_string(yml, merge_anchors: true) do
      {:ok, config} ->
        Repo.transaction(
          fn ->
            case apply_config(account_id, config, opts) do
              {:ok, resources} -> {:ok, resources}
              {:error, error} -> Repo.rollback(error)
            end
          end,
          timeout: :timer.seconds(90)
        )

      {:error, %YamlElixir.ParsingError{} = error} ->
        {:error, Error.bad_request(message: "Invalid YAML: #{Exception.message(error)}")}

      {:error, error} ->
        Logger.error("Error reading config file: #{inspect(error)}")
        {:error, error}
    end
  end

  def plan_from_yml(account_id \\ nil, yml, opts \\ []) do
    ## return a list of changesets
    case YamlElixir.read_from_string(yml, merge_anchors: true) do
      {:ok, config} ->
        result =
          Repo.transaction(
            fn ->
              account_id
              |> apply_config(config, opts)
              |> Repo.rollback()
            end,
            timeout: :timer.seconds(90)
          )

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

      {:error, %YamlElixir.ParsingError{} = error} ->
        {:error, Error.bad_request(message: "Invalid YAML: #{Exception.message(error)}")}

      {:error, error} when is_exception(error) ->
        {:error, Error.bad_request(message: "Error reading config file: #{Exception.message(error)}")}

      {:error, error} ->
        Logger.error("Error reading config file: #{inspect(error)}")
        {:error, Error.bad_request(message: "Error reading config file: #{inspect(error, pretty: true)}")}
    end
  end

  defp apply_config(account_id, config, opts) do
    with {:ok, account} <- find_or_create_account(account_id, config),
         {:ok, _users} <- find_or_create_users(account, config),
         {:ok, _tokens} <- find_or_create_tokens(account, config),
         {:ok, _databases} <- upsert_databases(account.id, config, opts),
         databases = Databases.list_dbs_for_account(account.id, [:sequences, :replication_slot]),
         {:ok, _wal_pipelines} <- upsert_wal_pipelines(account.id, config, databases),
         {:ok, _http_endpoints} <- upsert_http_endpoints(account.id, config),
         http_endpoints = Consumers.list_http_endpoints_for_account(account.id),
         {:ok, _transforms} <- upsert_transforms(account.id, config),
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
    transforms = Consumers.list_transforms_for_account(account_id)

    sink_consumers =
      account_id
      |> Consumers.list_sink_consumers_for_account(sequence: [:postgres_database])
      |> Enum.map(&SinkConsumer.preload_http_endpoint/1)

    [account | users] ++ databases ++ wal_pipelines ++ http_endpoints ++ transforms ++ sink_consumers
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

  ############
  ## Tokens ##
  ############

  defp find_or_create_tokens(account, %{"api_tokens" => tokens}) do
    if self_hosted?() do
      do_find_or_create_tokens(account, tokens)
    else
      {:error, Error.unauthorized(message: "api token creation is not supported in Sequin Cloud")}
    end
  end

  defp find_or_create_tokens(_, _), do: {:ok, []}

  defp do_find_or_create_tokens(account, tokens) do
    Enum.reduce_while(tokens, {:ok, []}, fn tok, {:ok, acc} ->
      case find_or_create_token(account, tok) do
        {:ok, token} -> {:cont, {:ok, [token | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp find_or_create_token(account, %{"name" => token_name} = params) do
    case ApiTokens.get_token_by(account_id: account.id, name: token_name) do
      {:ok, t} ->
        if t.token == params["token"] do
          {:ok, t}
        else
          {:error, Error.bad_request(message: "Cannot modify existing token: #{token_name}")}
        end

      {:error, _} ->
        ApiTokens.create_for_account(account.id, params)
    end
  end

  defp find_or_create_token(_, token) do
    {:error,
     Error.bad_request(
       message: "Error creating token: `name` required - received params: #{inspect(token, pretty: true)}"
     )}
  end

  ###############
  ## Databases ##
  ###############

  @database_defaults %{
    "username" => "postgres",
    "password" => "postgres",
    "port" => 5432
  }

  defp upsert_databases(account_id, %{"databases" => databases}, opts) do
    Logger.info("Upserting databases: #{inspect(databases, pretty: true)}")

    Enum.reduce_while(databases, {:ok, []}, fn database_attrs, {:ok, acc} ->
      database_attrs = Map.delete(database_attrs, "tables")

      case upsert_database(account_id, database_attrs, opts) do
        {:ok, database} -> {:cont, {:ok, [database | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_databases(_account_id, %{}, _opts) do
    Logger.info("No databases found in config")
    {:ok, []}
  end

  defp upsert_database(account_id, %{"name" => name} = database_attrs, opts) when is_binary(name) do
    test_connect_fun = Keyword.get(opts, :test_connect_fun, &Databases.test_connect/1)

    with {:ok, parsed_params} <- Sequin.Transforms.parse_db_params(database_attrs) do
      db_params = Map.merge(database_attrs, parsed_params)

      account_id
      |> Databases.get_db_for_account(name)
      |> case do
        {:ok, database} ->
          Logger.info("Found database: #{inspect(database, pretty: true)}")
          update_database(database, db_params)

        {:error, %NotFoundError{}} ->
          db_params_with_defaults = Map.merge(@database_defaults, db_params)

          with %Ecto.Changeset{valid?: true} <-
                 Databases.create_db_changeset(account_id, db_params_with_defaults),
               :ok <- await_database(db_params_with_defaults, test_connect_fun) do
            create_database_with_replication(account_id, db_params_with_defaults)
          else
            %Ecto.Changeset{valid?: false} = changeset ->
              # To get well-formatted errors, convert to ValidationError first
              error = Error.validation(changeset: changeset)

              {:error, Error.bad_request(message: "Error creating database '#{name}': #{Exception.message(error)}")}

            error ->
              error
          end
      end
    end
  end

  defp upsert_database(_account_id, _, _opts) do
    {:error, Error.bad_request(message: "Database name is required for each database")}
  end

  defp await_database(database_attrs, test_connect_fun, started_at \\ nil) do
    now = :erlang.system_time(:millisecond)
    started_at = started_at || now

    name = database_attrs["name"]
    await_database = database_attrs["await_database"] || %{}
    max_wait = await_database["timeout_ms"] || default_await_database_opts()[:timeout_ms]
    interval = await_database["interval_ms"] || default_await_database_opts()[:interval_ms]

    timeout_at = started_at + max_wait

    valid_keys = %PostgresDatabase{} |> Map.keys() |> Enum.map(&Atom.to_string/1)

    attrs =
      database_attrs
      |> Map.take(valid_keys)
      |> Map.new(fn {k, v} -> {String.to_existing_atom(k), v} end)

    pd = struct(PostgresDatabase, attrs)

    case test_connect_fun.(pd) do
      :ok ->
        :ok

      {:error, error} ->
        if now < timeout_at do
          Logger.info("Waiting for external database (#{name}) to be ready...")
          remaining_time = timeout_at - now
          sleep_time = min(interval, remaining_time)
          Process.sleep(sleep_time)
          await_database(database_attrs, test_connect_fun, started_at)
        else
          if is_exception(error) do
            Logger.error("Failed to connect to database: #{Exception.message(error)}")
          else
            Logger.error("Failed to connect to database: #{inspect(error, pretty: true)}")
          end

          {:error,
           Error.service(
             message:
               "Failed to connect to database '#{name}' after #{max_wait}ms. Please check your database credentials.",
             service: :customer_postgres
           )}
        end
    end
  end

  defp create_database_with_replication(account_id, database) do
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
      status: Transforms.parse_status(attrs["status"]),
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
    Enum.map(filters, &Transforms.parse_column_filter(&1, table))
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

        {:error, error} when is_exception(error) ->
          {:halt,
           {:error, Error.bad_request(message: "Invalid HTTP endpoint configuration: #{Exception.message(error)}")}}

        {:error, %Ecto.Changeset{} = changeset} ->
          {:halt,
           {:error,
            Error.bad_request(message: "Invalid HTTP endpoint configuration: #{inspect(changeset, pretty: true)}")}}
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
    with {:ok, endpoint_params} <- Transforms.from_external_http_endpoint(attrs),
         {:ok, endpoint} <- Sequin.Consumers.create_http_endpoint(account_id, endpoint_params) do
      Logger.info("Created HTTP endpoint: #{inspect(endpoint, pretty: true)}")
      {:ok, endpoint}
    else
      {:error, error} when is_exception(error) ->
        {:error,
         Error.bad_request(
           message: "Error creating HTTP endpoint '#{attrs["name"]}': #{Exception.message(error)}\n#{@http_endpoint_docs}"
         )}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(
           message:
             "Error creating HTTP endpoint '#{attrs["name"]}': #{inspect(changeset, pretty: true)}\n#{@http_endpoint_docs}"
         )}
    end
  end

  defp update_http_endpoint(endpoint, attrs) do
    with {:ok, endpoint_params} <- Transforms.from_external_http_endpoint(attrs),
         {:ok, endpoint} <- Sequin.Consumers.update_http_endpoint(endpoint, endpoint_params) do
      Logger.info("Updated HTTP endpoint: #{inspect(endpoint, pretty: true)}")
      {:ok, endpoint}
    else
      {:error, error} ->
        {:error,
         Error.bad_request(
           message: """
           Error updating HTTP endpoint '#{endpoint.name}': #{Exception.message(error)}

           #{@http_endpoint_docs}
           """
         )}
    end
  end

  #########################
  ## HTTP Push Consumers ##
  #########################

  defp upsert_sink_consumers(account_id, %{"sinks" => consumers}, databases, http_endpoints) do
    Logger.info("Upserting sink consumers: #{inspect(consumers, pretty: true)}")

    Enum.reduce_while(consumers, {:ok, []}, fn consumer, {:ok, acc} ->
      # This is a hack while sequences might be created while we create sinks
      # TODO: Excise this once sequences are removed
      databases = Repo.preload(databases, [:sequences], force: true)

      case upsert_sink_consumer(account_id, consumer, databases, http_endpoints) do
        {:ok, consumer} -> {:cont, {:ok, [consumer | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  rescue
    e in Postgrex.Error ->
      case e do
        %_{postgres: %{message: "routing_id must reference a transform with type 'routing'"}} ->
          raise "`routing` must reference a transform with type `routing`"

        _ ->
          reraise e, __STACKTRACE__
      end
  end

  defp upsert_sink_consumers(_account_id, %{}, _databases, _http_endpoints), do: {:ok, []}

  defp upsert_sink_consumer(account_id, %{"name" => name} = consumer_attrs, databases, http_endpoints) do
    # Find existing consumer first
    case Sequin.Consumers.find_sink_consumer(account_id, name: name) do
      {:ok, existing_consumer} ->
        with {:ok, params} <-
               Transforms.from_external_sink_consumer(account_id, consumer_attrs, databases, http_endpoints),
             {:ok, consumer} <- Sequin.Consumers.update_sink_consumer(existing_consumer, params) do
          Logger.info("Updated HTTP push consumer: #{inspect(consumer, pretty: true)}")
          {:ok, consumer}
        end

      {:error, %NotFoundError{}} ->
        with {:ok, params} <-
               Transforms.from_external_sink_consumer(account_id, consumer_attrs, databases, http_endpoints),
             {:ok, consumer} <-
               Sequin.Consumers.create_sink_consumer(account_id, params) do
          Logger.info("Created HTTP push consumer: #{inspect(consumer, pretty: true)}")
          {:ok, consumer}
        end
    end
  end

  ###############
  ## Utilities ##
  ###############

  defp config_file_yaml do
    Application.get_env(@app, :config_file_yaml)
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

  defp default_await_database_opts do
    if env() == :test do
      %{
        timeout_ms: 5000,
        interval_ms: 1
      }
    else
      %{
        timeout_ms: :timer.seconds(30),
        interval_ms: :timer.seconds(3)
      }
    end
  end

  defp upsert_transforms(_, %{"transforms" => _, "functions" => _}) do
    {:error, "Cannot specify both `functions` and `transforms`"}
  end

  defp upsert_transforms(account_id, %{"transforms" => transforms}) do
    upsert_transforms(account_id, %{"functions" => transforms})
  end

  defp upsert_transforms(account_id, %{"functions" => transforms}) do
    Logger.info("Upserting transforms: #{inspect(transforms, pretty: true)}")

    Enum.reduce_while(transforms, {:ok, []}, fn transform_attrs, {:ok, acc} ->
      case upsert_transform(account_id, transform_attrs) do
        {:ok, transform} -> {:cont, {:ok, [transform | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_transforms(_account_id, %{}), do: {:ok, []}

  defp upsert_transform(account_id, %{"name" => name} = raw_attrs) do
    with {:ok, transform_attrs} <- coerce_transform_attrs(raw_attrs) do
      case Consumers.find_transform(account_id, name: name) do
        {:ok, transform} ->
          update_transform(account_id, transform.id, transform_attrs)

        {:error, %NotFoundError{}} ->
          create_transform(account_id, transform_attrs)
      end
    end
  end

  defp upsert_transform(_account_id, %{}) do
    {:error, Error.validation(summary: "`name` is required on transforms.")}
  end

  defp create_transform(account_id, attrs) do
    case Consumers.create_transform(account_id, attrs) do
      {:ok, transform} ->
        Logger.info("Created transform: #{inspect(transform, pretty: true)}")
        {:ok, transform}

      {:error, changeset} ->
        error = Sequin.Error.errors_on(changeset)

        {:error,
         Error.bad_request(message: "Error creating transform '#{attrs["name"]}': #{inspect(error, pretty: true)}")}
    end
  end

  defp update_transform(account_id, id, attrs) do
    case Consumers.update_transform(account_id, id, attrs) do
      {:ok, transform} ->
        Logger.info("Updated transform: #{inspect(transform, pretty: true)}")
        {:ok, transform}

      {:error, changeset} ->
        error = Sequin.Error.errors_on(changeset)

        {:error,
         Error.bad_request(message: "Error updating transform '#{attrs["name"]}': #{inspect(error, pretty: true)}")}
    end
  end

  defp coerce_transform_attrs(%{"function" => _, "transform" => _}) do
    {:error, "Cannot specify both `function` and `transform`"}
  end

  defp coerce_transform_attrs(%{"function" => transform} = raw_attrs) do
    attrs =
      raw_attrs
      |> Map.delete("function")
      |> Map.put("transform", coerce_transform_inner(transform))

    {:ok, attrs}
  end

  defp coerce_transform_attrs(%{"transform" => _} = attrs) do
    {:ok, Map.update!(attrs, "transform", &coerce_transform_inner/1)}
  end

  # Assume that if you don't have "function" or "transform" that you used flat structure
  defp coerce_transform_attrs(flat) do
    nested_attrs =
      flat
      |> Map.take(["id", "name"])
      |> Map.put("transform", Map.take(flat, ["type", "sink_type", "code", "description"]))

    {:ok, nested_attrs}
  end

  defp coerce_transform_inner(%{"sink_type" => "webhook"} = attrs) do
    Map.put(attrs, "sink_type", "http_push")
  end

  defp coerce_transform_inner(attrs), do: attrs
end
