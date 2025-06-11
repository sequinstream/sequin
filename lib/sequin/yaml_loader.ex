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
  alias Sequin.Error.ServiceError
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Transforms

  require Logger

  @app :sequin

  defmodule Action do
    @moduledoc false
    @derive {Jason.Encoder, only: [:description]}
    defstruct [:description, :action]
  end

  def apply_from_stdin! do
    load_app()
    ensure_repo_started!()

    Logger.info("Reading config from stdin")

    case IO.read(:stdio, :eof) do
      :eof ->
        Logger.info("No config data received from stdin")

      {:error, reason} ->
        raise "Failed to read config from stdin: #{inspect(reason)}"

      yml when is_binary(yml) ->
        Logger.info("Received config data, applying...")
        apply_from_yml!(yml)
    end

    :ok
  end

  def apply_from_yml!(account_id \\ nil, yml) do
    case apply_from_yml(account_id, yml) do
      {:ok, _resources} ->
        :ok

      {:error, error} ->
        raise "Failed to apply config: #{inspect(error)}"
    end
  end

  def apply_from_yml(account_id \\ nil, yml, opts \\ []) do
    case YamlElixir.read_from_string(yml, merge_anchors: true) do
      {:ok, config} ->
        case Repo.transaction(
               fn ->
                 case apply_config(account_id, config, opts) do
                   {:ok, resources, actions} ->
                     {:ok, {resources, actions}}

                   {:error, error} ->
                     Repo.rollback(error)
                 end
               end,
               timeout: :timer.seconds(90)
             ) do
          {:ok, {:ok, {resources, actions}}} ->
            case perform_actions(actions) do
              :ok ->
                {:ok, resources}

              {:error, errors} ->
                msg = """
                Successfully applied config, but failed to apply actions:

                #{Enum.map_join(errors, "\n", &Exception.message/1)}
                """

                {:error, Error.invariant(message: msg)}
            end

          {:error, error} ->
            {:error, map_error(error)}
        end

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
          {:error, {:ok, planned_resources, actions}} ->
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
            {:ok, planned_resources, current_resources, actions}

          {:error, {:error, error}} ->
            {:error, map_error(error)}
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

  defp map_error(%Ecto.Changeset{} = changeset) do
    case Error.errors_on(changeset) do
      %{source_tables: [%{group_column_attnums: [error | _]} | _]} ->
        Error.validation(summary: "Invalid table.group_column_names: #{error}")

      _ ->
        Error.validation(changeset: changeset)
    end
  end

  defp map_error(error), do: error

  defp apply_config(account_id, config, opts) do
    with {:ok, account} <- find_or_create_account(account_id, config),
         {:ok, _users} <- find_or_create_users(account, config),
         {:ok, _tokens} <- find_or_create_tokens(account, config),
         {:ok, _databases, actions} <- upsert_databases(account.id, config, opts),
         databases = Databases.list_dbs_for_account(account.id, [:replication_slot]),
         {:ok, _wal_pipelines} <- upsert_wal_pipelines(account.id, config, databases),
         {:ok, _http_endpoints} <- upsert_http_endpoints(account.id, config),
         http_endpoints = Consumers.list_http_endpoints_for_account(account.id),
         {:ok, _functions} <- upsert_functions(account.id, config),
         {:ok, _sink_consumers} <- upsert_sink_consumers(account.id, config, databases, http_endpoints) do
      {:ok, all_resources(account.id), actions}
    end
  end

  def all_resources(nil), do: []

  def all_resources(account_id) do
    account = Accounts.get_account!(account_id)
    users = Accounts.list_users_for_account(account_id)
    databases = Databases.list_dbs_for_account(account_id, [:replication_slot])
    wal_pipelines = Replication.list_wal_pipelines_for_account(account_id, [:source_database, :destination_database])
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)
    functions = Consumers.list_functions_for_account(account_id)

    sink_consumers =
      account_id
      |> Consumers.list_sink_consumers_for_account(:postgres_database)
      |> Enum.map(&SinkConsumer.preload_http_endpoint!/1)

    [account | users] ++ databases ++ wal_pipelines ++ http_endpoints ++ functions ++ sink_consumers
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

  defp do_find_or_create_account(%{"account" => %{"name" => nil}}) do
    {:error, Error.bad_request(message: "Account name is required.")}
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

  defp find_or_create_token(_account, %{"name" => nil} = _params) do
    {:error, Error.validation(summary: "`name` is required on each token.")}
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

    Enum.reduce_while(databases, {:ok, [], []}, fn database_attrs, {:ok, acc_databases, acc_actions} ->
      database_attrs = Map.delete(database_attrs, "tables")

      case upsert_database(account_id, database_attrs, opts) do
        {:ok, database} -> {:cont, {:ok, [database | acc_databases], acc_actions}}
        {:ok, database, actions} -> {:cont, {:ok, [database | acc_databases], actions ++ acc_actions}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_databases(_account_id, %{}, _opts) do
    Logger.info("No databases found in config")
    {:ok, [], []}
  end

  defp upsert_database(_account_id, %{"name" => nil} = _database_attrs, _opts) do
    {:error, Error.bad_request(message: "Database name is required.")}
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

          database = Sequin.Repo.preload(database, :replication_slot)

          with {:ok, database_attrs} <- parse_database_update_attrs(db_params, database.replication_slot) do
            update_database(database, database_attrs)
          end

        {:error, %NotFoundError{}} ->
          db_params_with_defaults = Map.merge(@database_defaults, db_params)

          with {:ok, db_params_with_defaults} <- parse_database_update_attrs(db_params_with_defaults, nil),
               %Ecto.Changeset{valid?: true} <- Databases.create_db_changeset(account_id, db_params_with_defaults),
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

  defp create_database_with_replication(account_id, database_attrs) do
    with {:ok, database_attrs, replication_attrs, slot_attrs, pub_attrs} <- parse_database_attrs(database_attrs),
         {:ok, db} <- Databases.create_db(account_id, database_attrs) do
      create_pub_actions = maybe_create_publication_action(db, pub_attrs)
      create_slot_actions = maybe_create_slot_action(db, slot_attrs)

      replication_attrs = Map.put(replication_attrs, "postgres_database_id", db.id)

      validate_slot? = database_attrs["slot"]["create_if_not_exists"] != true
      validate_pub? = database_attrs["publication"]["create_if_not_exists"] != true

      case Replication.create_pg_replication(account_id, replication_attrs,
             validate_slot?: validate_slot?,
             validate_pub?: validate_pub?
           ) do
        {:ok, replication} ->
          Logger.info("Created database: #{inspect(db, pretty: true)}")
          {:ok, %PostgresDatabase{db | replication_slot: replication}, create_pub_actions ++ create_slot_actions}

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
    else
      {:error, %DBConnection.ConnectionError{}} ->
        {:error,
         Error.bad_request(
           message: "Failed to connect to database '#{database_attrs["name"]}'. Please check your database credentials."
         )}

      {:error, error} when is_exception(error) ->
        {:error,
         Error.bad_request(message: "Error creating database '#{database_attrs["name"]}': #{Exception.message(error)}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(
           message: "Error creating database '#{database_attrs["name"]}': #{inspect(changeset, pretty: true)}"
         )}
    end
  end

  defp parse_database_update_attrs(%{"publication" => _, "publication_name" => _}, _replication_slot) do
    {:error,
     Error.validation(
       summary:
         "Invalid database configuration: `publication` and `publication_name` are both specified. Only `publication` should be used."
     )}
  end

  defp parse_database_update_attrs(%{"slot" => _, "slot_name" => _}, _replication_slot) do
    {:error,
     Error.validation(
       summary: "Invalid database configuration: `slot` and `slot_name` are both specified. Only `slot` should be used."
     )}
  end

  defp parse_database_update_attrs(database_attrs, replication_slot) do
    slot_name = database_attrs["slot"]["name"] || database_attrs["slot_name"]
    pub_name = database_attrs["publication"]["name"] || database_attrs["publication_name"]

    cond do
      not is_nil(slot_name) and not is_nil(replication_slot) and slot_name != replication_slot.slot_name ->
        {:error,
         Error.validation(
           summary:
             "Updating a database's replication slot is not supported. You can update in the Sequin web console. (Current slot name: #{replication_slot.slot_name}, new slot name: #{slot_name})"
         )}

      not is_nil(pub_name) and not is_nil(replication_slot) and pub_name != replication_slot.publication_name ->
        {:error,
         Error.validation(
           summary:
             "Updating a database's publication is not supported. You can update in the Sequin web console. (Current publication name: #{replication_slot.publication_name}, new publication name: #{pub_name})"
         )}

      true ->
        {:ok, database_attrs}
    end
  end

  defp parse_database_attrs(database_attrs) do
    cond do
      # Check for mutual exclusion: slot block vs slot_name flat key
      # Flat "slot_name" is old style
      is_nil(database_attrs["slot"]) and is_nil(database_attrs["slot_name"]) ->
        {:error, Error.validation(summary: "`slot` is not specified")}

      # Check for mutual exclusion: publication block vs publication_name flat key
      # Flat "publication_name" is old style
      is_nil(database_attrs["publication"]) and is_nil(database_attrs["publication_name"]) ->
        {:error, Error.validation(summary: "`publication` is not specified")}

      true ->
        slot_name = database_attrs["slot_name"] || database_attrs["slot"]["name"]
        pub_name = database_attrs["publication_name"] || database_attrs["publication"]["name"]

        replication_attrs = %{"slot_name" => slot_name, "publication_name" => pub_name}

        slot_attrs =
          case Map.fetch(database_attrs, "slot") do
            {:ok, %{"create_if_not_exists" => true}} -> %{"name" => slot_name}
            _ -> nil
          end

        pub_attrs =
          case Map.fetch(database_attrs, "publication") do
            {:ok, %{"create_if_not_exists" => true}} ->
              %{
                "name" => pub_name,
                "init_sql" => database_attrs["publication"]["init_sql"]
              }

            _ ->
              nil
          end

        database_attrs = Map.merge(database_attrs, %{"slot_name" => slot_name, "publication_name" => pub_name})

        {:ok, database_attrs, replication_attrs, slot_attrs, pub_attrs}
    end
  end

  defp maybe_create_slot_action(_, nil), do: []

  defp maybe_create_slot_action(db, slot_attrs) do
    go = fn -> maybe_create_replication_slot(db, slot_attrs) end
    [%__MODULE__.Action{action: go, description: "Ensure replication slot `#{slot_attrs["name"]}` exists"}]
  end

  defp maybe_create_publication_action(_, nil), do: []

  defp maybe_create_publication_action(db, pub_attrs) do
    go = fn -> maybe_create_publication(db, pub_attrs) end
    [%__MODULE__.Action{action: go, description: "Ensure publication `#{pub_attrs["name"]}` exists"}]
  end

  defp maybe_create_replication_slot(db, slot_params) do
    case Postgres.create_replication_slot(db, slot_params["name"]) do
      :ok ->
        :ok

      {:error, %ServiceError{} = error} ->
        {:error,
         Error.bad_request(
           message: "Error creating replication slot: #{error.message} (#{inspect(error.details, pretty: true)})"
         )}
    end
  end

  defp maybe_create_publication(db, pub_params) do
    case Postgres.create_publication(db, pub_params["name"], pub_params["init_sql"]) do
      :ok ->
        :ok

      {:error, %ServiceError{} = error} ->
        {:error,
         Error.bad_request(
           message: "Error creating publication: #{error.message} (#{inspect(error.details, pretty: true)})"
         )}

      {:error, error} ->
        {:error, Error.bad_request(message: "Error creating publication: #{Exception.message(error)}")}
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

  defp upsert_wal_pipeline(_account_id, %{"name" => nil} = _attrs, _databases) do
    {:error, Error.validation(summary: "`name` is required on each wal_pipeline.")}
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
    case parse_wal_pipeline_attrs(attrs, databases) do
      {:ok, params} ->
        Replication.create_wal_pipeline_with_lifecycle(account_id, params)

      {:error, error} when is_exception(error) ->
        {:error,
         Error.bad_request(message: "Error setting up change retention '#{attrs["name"]}': #{Exception.message(error)}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(
           message: "Error setting up change retention '#{attrs["name"]}': #{inspect(changeset, pretty: true)}"
         )}

      {:error, error} ->
        {:error, error}
    end
  end

  defp update_wal_pipeline(wal_pipeline, attrs, databases) do
    with {:ok, params} <- parse_wal_pipeline_attrs(attrs, databases),
         {:ok, wal_pipeline} <- Replication.update_wal_pipeline_with_lifecycle(wal_pipeline, params) do
      Logger.info("Updated change retention: #{inspect(wal_pipeline, pretty: true)}")
      {:ok, wal_pipeline}
    else
      {:error, error} when is_exception(error) ->
        {:error,
         Error.bad_request(message: "Error updating change retention '#{wal_pipeline.name}': #{Exception.message(error)}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error,
         Error.bad_request(
           message: "Error updating change retention '#{wal_pipeline.name}': #{inspect(changeset, pretty: true)}"
         )}

      {:error, error} ->
        {:error, error}
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
    with {:ok, source_database} <- fetch_database(databases, source_db_name, :source_database),
         {:ok, destination_database} <- fetch_database(databases, dest_db_name, :destination_database),
         {:ok, destination_table} <-
           fetch_table(destination_database.tables, dest_schema, dest_table, :destination_table),
         {:ok, source_table_struct} <-
           fetch_table(source_database.tables, source_schema, source_table, :source_table),
         {:ok, column_filters} <-
           parse_column_filters(attrs["filters"], source_database, source_schema, source_table) do
      source_table_config = %{
        "schema_name" => source_schema,
        "table_name" => source_table,
        "oid" => source_table_struct.oid,
        "actions" => attrs["actions"] || [:insert, :update, :delete],
        "column_filters" => column_filters
      }

      params = %{
        name: name,
        status: Transforms.parse_status(attrs["status"]),
        replication_slot_id: source_database.replication_slot.id,
        destination_database_id: destination_database.id,
        destination_oid: destination_table.oid,
        source_tables: [source_table_config]
      }

      {:ok, params}
    end
  end

  # Helper to parse column filters for WAL pipeline
  defp parse_column_filters(nil, _database, _schema, _table), do: {:ok, []}

  defp parse_column_filters(filters, database, schema, table_name) when is_list(filters) do
    with {:ok, table} <- fetch_table(database.tables, schema, table_name, :filter_table) do
      filters
      |> Enum.reduce_while({:ok, []}, fn filter, {:ok, acc} ->
        try do
          case Transforms.parse_column_filter(filter, table) do
            {:ok, column_filter} -> {:cont, {:ok, [column_filter | acc]}}
            {:error, error} -> {:halt, {:error, error}}
          end
        rescue
          error -> {:halt, {:error, Error.bad_request(message: Exception.message(error))}}
        end
      end)
      |> case do
        {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
        {:error, error} -> {:error, error}
      end
    end
  end

  defp parse_column_filters(_, _database, _schema, _table),
    do: {:error, Error.bad_request(message: "`filters` must be a list")}

  defp fetch_database(databases, name, _role) do
    case Enum.find(databases, &(&1.name == name)) do
      nil -> {:error, Error.not_found(entity: :database, params: %{name: name})}
      db -> {:ok, db}
    end
  end

  defp fetch_table(tables, schema, table_name, _role) do
    case Enum.find(tables, &(&1.schema == schema && &1.name == table_name)) do
      nil -> {:error, Error.not_found(entity: :table, params: %{schema: schema, name: table_name})}
      table -> {:ok, table}
    end
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

  defp upsert_http_endpoint(_account_id, %{"name" => nil} = _attrs) do
    {:error, Error.validation(summary: "`name` is required on each http_endpoint.")}
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
    Enum.reduce_while(consumers, {:ok, []}, fn consumer, {:ok, acc} ->
      case upsert_sink_consumer(account_id, consumer, databases, http_endpoints) do
        {:ok, consumer} ->
          Logger.info("Upserted sink consumer", consumer_id: consumer.id)
          {:cont, {:ok, [consumer | acc]}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
  rescue
    e in Postgrex.Error ->
      case e do
        %_{postgres: %{message: "routing_id must reference a function with type 'routing'"}} ->
          raise "`routing` must reference a function with type `routing`"

        _ ->
          reraise e, __STACKTRACE__
      end
  end

  defp upsert_sink_consumers(_account_id, %{}, _databases, _http_endpoints), do: {:ok, []}

  defp upsert_sink_consumer(_account_id, %{"name" => nil} = _consumer_attrs, _databases, _http_endpoints) do
    {:error, Error.validation(summary: "`name` is a required field on each sink.")}
  end

  defp upsert_sink_consumer(account_id, %{"name" => name} = consumer_attrs, databases, http_endpoints) do
    # Find existing consumer first
    case Sequin.Consumers.find_sink_consumer(account_id, name: name) do
      {:ok, existing_consumer} ->
        with {:ok, params} <-
               Transforms.from_external_sink_consumer(account_id, consumer_attrs, databases, http_endpoints),
             params = ensure_function_keys_are_nil_when_not_specified(params),
             {:ok, consumer} <- Sequin.Consumers.update_sink_consumer(existing_consumer, params) do
          Logger.info("Updated HTTP push consumer", consumer_id: consumer.id)
          {:ok, consumer}
        end

      {:error, %NotFoundError{}} ->
        with {:ok, params} <-
               Transforms.from_external_sink_consumer(account_id, consumer_attrs, databases, http_endpoints),
             {:ok, consumer} <-
               Sequin.Consumers.create_sink_consumer(account_id, params) do
          Logger.info("Created HTTP push consumer", consumer_id: consumer.id)
          {:ok, consumer}
        end
    end
  end

  # Ensures that transform_id, filter_id, and routing_id are set to nil when not explicitly specified in the YAML.
  # This allows users to remove attached functions by omitting them from the YAML configuration.
  defp ensure_function_keys_are_nil_when_not_specified(params) do
    params
    |> Map.put_new(:transform_id, nil)
    |> Map.put_new(:filter_id, nil)
    |> Map.put_new(:routing_id, nil)
    |> Map.put_new(:routing_mode, "static")
  end

  ###############
  ## Utilities ##
  ###############

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

  defp upsert_functions(_, %{"transforms" => _, "functions" => _}) do
    {:error, "Cannot specify both `functions` and `transforms`"}
  end

  defp upsert_functions(account_id, %{"transforms" => functions}) do
    upsert_functions(account_id, %{"functions" => functions})
  end

  defp upsert_functions(account_id, %{"functions" => functions}) do
    Logger.info("Upserting functions: #{inspect(functions, pretty: true)}")

    Enum.reduce_while(functions, {:ok, []}, fn function_attrs, {:ok, acc} ->
      case upsert_function(account_id, function_attrs) do
        {:ok, function} -> {:cont, {:ok, [function | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp upsert_functions(_account_id, %{}), do: {:ok, []}

  defp upsert_function(_account_id, %{"name" => nil} = _function_attrs) do
    {:error, Error.validation(summary: "`name` is required on functions.")}
  end

  defp upsert_function(account_id, %{"name" => name} = raw_attrs) do
    with {:ok, function_attrs} <- coerce_function_attrs(raw_attrs) do
      case Consumers.find_function(account_id, name: name) do
        {:ok, function} ->
          update_function(account_id, function.id, function_attrs)

        {:error, %NotFoundError{}} ->
          create_function(account_id, function_attrs)
      end
    end
  end

  defp upsert_function(_account_id, %{}) do
    {:error, Error.validation(summary: "`name` is required on functions.")}
  end

  defp create_function(account_id, attrs) do
    case Consumers.create_function(account_id, attrs) do
      {:ok, function} ->
        Logger.info("Created function: #{inspect(function, pretty: true)}")
        {:ok, function}

      {:error, changeset} ->
        error = Sequin.Error.errors_on(changeset)

        {:error,
         Error.bad_request(message: "Error creating function '#{attrs["name"]}': #{inspect(error, pretty: true)}")}
    end
  end

  defp update_function(account_id, id, attrs) do
    case Consumers.update_function(account_id, id, attrs) do
      {:ok, function} ->
        Logger.info("Updated function: #{inspect(function, pretty: true)}")
        {:ok, function}

      {:error, changeset} ->
        error = Sequin.Error.errors_on(changeset)

        {:error,
         Error.bad_request(message: "Error updating function '#{attrs["name"]}': #{inspect(error, pretty: true)}")}
    end
  end

  defp coerce_function_attrs(%{"function" => _, "transform" => _}) do
    {:error, "Cannot specify both `function` and `transform`"}
  end

  defp coerce_function_attrs(%{"transform" => function} = raw_attrs) do
    attrs =
      raw_attrs
      |> Map.delete("transform")
      |> Map.put("function", coerce_sink_type(function))
      |> update_in(["function", "type"], &coerce_type_to_transform/1)

    {:ok, attrs}
  end

  defp coerce_function_attrs(%{"function" => _} = attrs) do
    {:ok, Map.update!(attrs, "function", &coerce_sink_type/1)}
  end

  # Assume that if you don't have "function" or "function" that you used flat structure
  defp coerce_function_attrs(flat) do
    inner =
      flat
      |> Map.take(["type", "sink_type", "code", "description", "path"])
      |> coerce_sink_type()
      |> Map.update("type", nil, &coerce_type_to_transform/1)

    nested_attrs =
      flat
      |> Map.take(["id", "name"])
      |> Map.put("function", inner)

    {:ok, nested_attrs}
  end

  # Helper function to coerce "function" type to "transform" for backwards compatibility
  defp coerce_type_to_transform("function"), do: "transform"
  defp coerce_type_to_transform(type), do: type

  defp coerce_sink_type(%{"sink_type" => "webhook"} = attrs) do
    Map.put(attrs, "sink_type", "http_push")
  end

  defp coerce_sink_type(attrs), do: attrs

  defp perform_actions(actions) do
    Enum.reduce(actions, :ok, fn %__MODULE__.Action{} = action, status_tuple ->
      case {action.action.(), status_tuple} do
        {:ok, :ok} -> :ok
        {{:ok, _}, :ok} -> :ok
        {{:ok, _}, {:error, errors}} -> {:error, errors}
        {{:error, error}, :ok} -> {:error, [error]}
        {{:error, error}, {:error, errors}} -> {:error, [error | errors]}
      end
    end)
  end
end
