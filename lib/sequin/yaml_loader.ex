defmodule Sequin.YamlLoader do
  @moduledoc false
  import Ecto.Changeset, only: [get_field: 2]

  alias Ecto.Changeset
  alias Sequin.Accounts
  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.WebhookSiteGenerator
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.KeysetCursor
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
    case YamlElixir.read_from_string(yml) do
      {:ok, config} ->
        result =
          Repo.transaction(fn ->
            account = find_or_create_account!(config)
            _users = find_or_create_users!(account, config)

            _databases = upsert_databases!(account.id, config)
            databases = Databases.list_dbs_for_account(account.id)

            _sequences = find_or_create_sequences!(account.id, config, databases)
            databases = Databases.list_dbs_for_account(account.id, [:sequences, :replication_slot])

            _http_endpoints = upsert_http_endpoints!(account.id, config)
            http_endpoints = Consumers.list_http_endpoints_for_account(account.id)

            # Not implemented
            _http_push_consumers = upsert_http_push_consumers!(account.id, config, databases, http_endpoints)
            _http_pull_consumers = upsert_http_pull_consumers!(account.id, config, databases)
          end)

        case result do
          {:ok, _} -> :ok
          {:error, error} -> raise "Failed to apply config: #{inspect(error)}"
        end

      {:error, error} ->
        Logger.error("Error reading config file: #{inspect(error)}")
        raise "Error reading config file: #{inspect(error)}"
    end
  end

  def plan_from_yml(yml) do
    ## return a list of changesets
    case YamlElixir.read_from_string(yml) do
      {:ok, config} ->
        with %Changeset{valid?: true} = account_changeset <- parse_account_config(config),
             database_changesets = parse_databases_config(account_changeset.data.id, config),
             [] <- Enum.reject(database_changesets, & &1.valid?) do
          users_changesets = parse_users_config(account_changeset.data.id, config)
          sequence_changesets = parse_sequences_config(account_changeset.data.id, config, database_changesets)
          http_endpoint_changesets = parse_http_endpoints_config(account_changeset.data.id, config)

          valid_changesets =
            [account_changeset] ++
              users_changesets ++
              database_changesets ++
              sequence_changesets ++
              http_endpoint_changesets

          {:ok, valid_changesets}
        else
          %Changeset{valid?: false} = invalid_changeset ->
            {:error, [invalid_changeset]}

          invalid_changesets when is_list(invalid_changesets) ->
            {:error, invalid_changesets}
        end

      {:error, error} ->
        Logger.error("Error reading config file: #{inspect(error)}")
        raise "Error reading config file: #{inspect(error)}"
    end
  end

  #############
  ## Account ##
  #############

  defp parse_account_config(config) do
    if self_hosted?() do
      do_parse_account_config(config)
    else
      raise "account configuration is not supported in Sequin Cloud"
    end
  end

  defp do_parse_account_config(%{"account" => %{"name" => name}}) do
    case Accounts.find_account(name: name) do
      {:ok, account} ->
        %{Accounts.Account.changeset(account, %{}) | action: :update}

      {:error, %NotFoundError{}} ->
        %{Accounts.Account.changeset(%Accounts.Account{}, %{name: name}) | action: :create}
    end
  end

  defp find_or_create_account!(config) do
    if self_hosted?() do
      do_find_or_create_account!(config)
    else
      raise "account configuration is not supported in Sequin Cloud"
    end
  end

  defp do_find_or_create_account!(%{"account" => %{"name" => name}}) do
    case Accounts.find_account(name: name) do
      {:ok, account} ->
        Logger.info("Found account: #{inspect(account, pretty: true)}")
        account

      {:error, %NotFoundError{}} ->
        {:ok, account} = Accounts.create_account(%{name: name})
        Logger.info("Created account: #{inspect(account, pretty: true)}")
        account
    end
  end

  defp do_find_or_create_account!(%{}) do
    raise "Account configuration is required."
  end

  ###########
  ## Users ##
  ###########

  defp parse_users_config(account_id, %{"users" => user_attrs}) do
    users = if account_id, do: Accounts.list_users_for_account(account_id), else: []

    Enum.map(user_attrs, &parse_user_config(&1, users))
  end

  defp parse_users_config(_account_id, %{}) do
    Logger.info("No users found in config")
    []
  end

  defp parse_user_config(%{"email" => email} = user_attrs, users) do
    case Enum.find(users, fn user -> user.email == email end) do
      nil -> %{Accounts.User.registration_changeset(%Accounts.User{}, user_attrs) | action: :create}
      user -> %{Accounts.User.update_changeset(user, user_attrs) | action: :update}
    end
  end

  defp find_or_create_users!(account, %{"users" => users}) do
    Logger.info("Creating users: #{inspect(users, pretty: true)}")
    Enum.map(users, &find_or_create_user!(account, &1))
  end

  defp find_or_create_users!(_account, %{}) do
    Logger.info("No users found in config")
    []
  end

  defp find_or_create_user!(account, %{"email" => email} = user_attrs) do
    case Accounts.get_user_by_email(:identity, email) do
      nil -> create_user!(account, user_attrs)
      user -> user
    end
  end

  defp create_user!(account, %{"email" => email, "password" => password}) do
    user_params = %{
      email: email,
      password: password,
      password_confirmation: password
    }

    case Accounts.register_user(:identity, user_params, account) do
      {:ok, user} ->
        user

      {:error, error} ->
        raise "Failed to create user: #{inspect(error)}"
    end
  end

  ###############
  ## Databases ##
  ###############

  @database_defaults %{
    "username" => "postgres",
    "password" => "postgres",
    "port" => 5432
  }

  defp parse_databases_config(account_id, %{"databases" => database_attrs}) do
    databases = if account_id, do: Databases.list_dbs_for_account(account_id), else: []

    Enum.map(database_attrs, &parse_database_config(&1, databases))
  end

  defp parse_database_config(%{"name" => name} = database_attrs, databases) do
    case Enum.find(databases, fn database -> database.name == name end) do
      nil -> %{PostgresDatabase.changeset(%PostgresDatabase{}, database_attrs) | action: :create}
      database -> %{PostgresDatabase.changeset(database, database_attrs) | action: :update}
    end
  end

  defp parse_database_config(_database_attrs, _databases) do
    PostgresDatabase.changeset(%PostgresDatabase{}, %{name: nil})
  end

  defp upsert_databases!(account_id, %{"databases" => databases}) do
    Logger.info("Upserting databases: #{inspect(databases, pretty: true)}")
    Enum.map(databases, &upsert_database!(account_id, &1))
  end

  defp upsert_databases!(_account_id, %{}) do
    Logger.info("No databases found in config")
    []
  end

  defp upsert_database!(account_id, %{"name" => name} = database_attrs) do
    account_id
    |> Databases.get_db_for_account(name)
    |> case do
      {:ok, database} ->
        Logger.info("Found database: #{inspect(database, pretty: true)}")
        update_database!(database, database_attrs)

      {:error, %NotFoundError{}} ->
        database = create_database_with_replication!(account_id, database_attrs)
        Logger.info("Created database: #{inspect(database, pretty: true)}")
        database
    end
  end

  defp create_database_with_replication!(account_id, database) do
    database = Map.merge(@database_defaults, database)

    account_id
    |> Databases.create_db_for_account_with_lifecycle(database)
    |> case do
      {:ok, db} ->
        replication_params = Map.put(database, "postgres_database_id", db.id)

        case Replication.create_pg_replication_for_account_with_lifecycle(account_id, replication_params) do
          {:ok, replication} ->
            Logger.info("Created database: #{inspect(db, pretty: true)}")
            %PostgresDatabase{db | replication_slot: replication}

          {:error, error} when is_exception(error) ->
            raise "Failed to create replication: #{Exception.message(error)}"

          {:error, %Ecto.Changeset{} = changeset} ->
            raise "Failed to create replication: #{inspect(changeset)}"
        end

      {:error, error} when is_exception(error) ->
        raise "Failed to create database: #{Exception.message(error)}"

      {:error, %Ecto.Changeset{} = changeset} ->
        raise "Failed to create database: #{inspect(changeset)}"
    end
  end

  defp update_database!(database, attrs) do
    case Databases.update_db(database, attrs) do
      {:ok, database} ->
        Logger.info("Updated database: #{inspect(database, pretty: true)}")
        database

      {:error, error} when is_exception(error) ->
        raise "Failed to update database: #{Exception.message(error)}"
    end
  end

  ###############
  ## Sequences ##
  ###############
  defp parse_sequences_config(account_id, %{"sequences" => sequences}, database_changesets) do
    existing_sequences = if account_id, do: Databases.list_sequences_for_account(account_id), else: []

    Enum.map(sequences, fn sequence ->
      database_changeset = find_database_changeset_for_sequence!(sequence, database_changesets)
      parse_sequence_config(sequence, database_changeset, existing_sequences)
    end)
  end

  defp parse_sequences_config(_account_id, %{}, _database_changesets), do: []

  defp find_database_changeset_for_sequence!(%{"database" => database_name}, database_changesets) do
    Enum.find(database_changesets, fn changeset ->
      get_field(changeset, :name) == database_name
    end) || raise "`database` must reference a valid database name"
  end

  defp parse_sequence_config(
         %{"table_schema" => table_schema, "table_name" => table_name} = sequence_attrs,
         database_changeset,
         existing_sequences
       ) do
    case Enum.find(existing_sequences, fn sequence ->
           sequence.table_schema == table_schema && sequence.table_name == table_name
         end) do
      nil ->
        # Create new sequence
        %{
          Sequence.changeset(%Sequence{}, %{
            table_schema: table_schema,
            table_name: table_name,
            sort_column_name: sequence_attrs["sort_column_name"],
            postgres_database_id: database_changeset.data.id
          })
          | action: :create
        }

      sequence ->
        # Update existing sequence
        %{
          Sequence.changeset(sequence, %{
            sort_column_name: sequence_attrs["sort_column_name"]
          })
          | action: :update
        }
    end
  end

  defp find_or_create_sequences!(account_id, %{"sequences" => sequences}, databases) do
    Logger.info("Creating sequences: #{inspect(sequences, pretty: true)}")

    Enum.map(sequences, fn sequence ->
      database = database_for_sequence!(sequence, databases)
      find_or_create_sequence!(account_id, database, sequence)
    end)
  end

  defp find_or_create_sequences!(_account_id, _config, _databases) do
    Logger.info("No sequences found in config")
    []
  end

  defp database_for_sequence!(%{"database" => database_name}, databases) do
    Sequin.Enum.find!(databases, fn database -> database.name == database_name end)
  end

  defp database_for_sequence!(%{}, _databases) do
    raise "`database` is required for each sequence and must be a valid database name"
  end

  defp find_or_create_sequence!(
         account_id,
         %PostgresDatabase{} = database,
         %{"table_schema" => table_schema, "table_name" => table_name} = sequence_attrs
       ) do
    case Databases.find_sequence_for_account(account_id, table_schema: table_schema, table_name: table_name) do
      {:ok, sequence} ->
        Logger.info("Found sequence: #{inspect(sequence, pretty: true)}")
        sequence

      {:error, %NotFoundError{}} ->
        create_sequence!(account_id, database, sequence_attrs)
    end
  end

  defp create_sequence!(account_id, %PostgresDatabase{id: id} = database, sequence) do
    table = table_for_sequence!(database, sequence)
    sort_column_attnum = sort_column_attnum_for_sequence!(table, sequence)

    attrs =
      sequence
      |> Map.put("postgres_database_id", id)
      |> Map.put("table_oid", table.oid)
      |> Map.put("sort_column_attnum", sort_column_attnum)

    account_id
    |> Databases.create_sequence(attrs)
    |> case do
      {:ok, sequence} ->
        Logger.info("Created sequence: #{inspect(sequence, pretty: true)}")
        sequence

      {:error, error} when is_exception(error) ->
        raise "Failed to create sequence: #{Exception.message(error)}"

      {:error, %Ecto.Changeset{} = changeset} ->
        raise "Failed to create sequence: #{inspect(changeset)}"
    end
  end

  defp table_for_sequence!(database, %{"table_schema" => table_schema, "table_name" => table_name}) do
    Sequin.Enum.find!(database.tables, fn table -> table.name == table_name and table.schema == table_schema end)
  end

  defp table_for_sequence!(_, %{}) do
    raise "`table` and `schema` are required for each sequence"
  end

  defp sort_column_attnum_for_sequence!(table, %{"sort_column_name" => sort_column_name}) do
    Sequin.Enum.find!(table.columns, fn column -> column.name == sort_column_name end).attnum
  end

  defp sort_column_attnum_for_sequence!(_, %{}) do
    raise "`sort_column_name` is required for each sequence"
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

  defp parse_http_endpoints_config(account_id, %{"http_endpoints" => http_endpoints}) do
    endpoints = if account_id, do: Sequin.Consumers.list_http_endpoints_for_account(account_id), else: []

    Enum.map(http_endpoints, fn endpoint_attrs ->
      parse_http_endpoint_config(endpoint_attrs, endpoints)
    end)
  end

  defp parse_http_endpoints_config(_account_id, %{}), do: []

  defp parse_http_endpoint_config(%{"name" => name} = attrs, endpoints) do
    case Enum.find(endpoints, fn endpoint -> endpoint.name == name end) do
      nil ->
        # Create new endpoint
        %{
          HttpEndpoint.create_changeset(
            %HttpEndpoint{},
            parse_http_endpoint_attrs(attrs)
          )
          | action: :create
        }

      endpoint ->
        # Update existing endpoint
        %{
          HttpEndpoint.update_changeset(
            endpoint,
            parse_http_endpoint_attrs(attrs)
          )
          | action: :update
        }
    end
  end

  defp upsert_http_endpoints!(account_id, %{"http_endpoints" => http_endpoints}) do
    Logger.info("Creating HTTP endpoints: #{inspect(http_endpoints, pretty: true)}")
    Enum.map(http_endpoints, &upsert_http_endpoint!(account_id, &1))
  end

  defp upsert_http_endpoints!(_account_id, %{}) do
    Logger.info("No HTTP endpoints found in config")
    []
  end

  defp upsert_http_endpoint!(account_id, %{"name" => name} = attrs) do
    case Sequin.Consumers.find_http_endpoint_for_account(account_id, name: name) do
      {:ok, endpoint} ->
        update_http_endpoint!(endpoint, attrs)

      {:error, %NotFoundError{}} ->
        create_http_endpoint!(account_id, attrs)
    end
  end

  defp create_http_endpoint!(account_id, attrs) do
    endpoint_params = parse_http_endpoint_attrs(attrs)

    case Sequin.Consumers.create_http_endpoint_for_account(account_id, endpoint_params) do
      {:ok, endpoint} ->
        Logger.info("Created HTTP endpoint: #{inspect(endpoint, pretty: true)}")
        endpoint

      {:error, error} when is_exception(error) ->
        raise "Failed to create HTTP endpoint: #{Exception.message(error)}"

      {:error, %Ecto.Changeset{} = changeset} ->
        raise "Failed to create HTTP endpoint: #{inspect(changeset)}"
    end
  end

  defp update_http_endpoint!(endpoint, attrs) do
    endpoint_params = parse_http_endpoint_attrs(attrs)

    case Sequin.Consumers.update_http_endpoint(endpoint, endpoint_params) do
      {:ok, endpoint} ->
        Logger.info("Updated HTTP endpoint: #{inspect(endpoint, pretty: true)}")
        endpoint

      {:error, error} when is_exception(error) ->
        raise "Failed to update HTTP endpoint: #{Exception.message(error)}"

      {:error, %Ecto.Changeset{} = changeset} ->
        raise "Failed to update HTTP endpoint: #{inspect(changeset)}"
    end
  end

  defp parse_http_endpoint_attrs(%{"name" => name} = attrs) do
    case attrs do
      # Webhook.site endpoint
      %{"webhook.site" => "true"} ->
        %{
          name: name,
          scheme: :https,
          host: "webhook.site",
          path: "/" <> generate_webhook_site_id()
        }

      # Local endpoint
      %{"local" => "true"} = local_attrs ->
        %{
          name: name,
          use_local_tunnel: true,
          path: local_attrs["path"],
          headers: parse_headers(local_attrs["headers"]),
          encrypted_headers: parse_headers(local_attrs["encrypted_headers"])
        }

      # External endpoint with URL
      %{"url" => url} = external_attrs ->
        uri = URI.parse(url)

        %{
          name: name,
          scheme: String.to_existing_atom(uri.scheme),
          host: uri.host,
          port: uri.port,
          path: uri.path,
          query: uri.query,
          fragment: uri.fragment,
          headers: parse_headers(external_attrs["headers"]),
          encrypted_headers: parse_headers(external_attrs["encrypted_headers"])
        }

      _ ->
        raise "Invalid HTTP endpoint configuration\n\n#{@http_endpoint_docs}"
    end
  end

  # Helper functions

  defp parse_headers(nil), do: %{}

  defp parse_headers(headers) when is_list(headers) do
    Map.new(headers, fn %{"key" => key, "value" => value} -> {key, value} end)
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

  defp upsert_http_push_consumers!(account_id, %{"webhook_subscriptions" => consumers}, databases, http_endpoints) do
    Logger.info("Upserting HTTP push consumers: #{inspect(consumers, pretty: true)}")
    Enum.map(consumers, &upsert_http_push_consumer!(account_id, &1, databases, http_endpoints))
  end

  defp upsert_http_push_consumers!(_account_id, %{}, _databases, _http_endpoints), do: []

  defp upsert_http_push_consumer!(account_id, %{"name" => name} = consumer_attrs, databases, http_endpoints) do
    # Find existing consumer first
    case Sequin.Consumers.find_http_push_consumer(account_id, name: name) do
      {:ok, existing_consumer} ->
        params = parse_http_push_consumer_params(consumer_attrs, databases, http_endpoints)

        case Sequin.Consumers.update_consumer_with_lifecycle(existing_consumer, params) do
          {:ok, consumer} ->
            Logger.info("Updated HTTP push consumer: #{inspect(consumer, pretty: true)}")
            consumer

          {:error, error} when is_exception(error) ->
            raise "Failed to update HTTP push consumer: #{Exception.message(error)}"

          {:error, %Ecto.Changeset{} = changeset} ->
            raise "Failed to update HTTP push consumer: #{inspect(changeset)}"
        end

      {:error, %NotFoundError{}} ->
        params = parse_http_push_consumer_params(consumer_attrs, databases, http_endpoints)

        case Sequin.Consumers.create_http_push_consumer_for_account_with_lifecycle(account_id, params) do
          {:ok, consumer} ->
            Logger.info("Created HTTP push consumer: #{inspect(consumer, pretty: true)}")
            consumer

          {:error, error} when is_exception(error) ->
            raise "Failed to create HTTP push consumer: #{Exception.message(error)}"

          {:error, %Ecto.Changeset{} = changeset} ->
            raise "Failed to create HTTP push consumer: #{inspect(changeset)}"
        end
    end
  end

  defp parse_http_push_consumer_params(
         %{"name" => name, "sequence" => sequence_name, "http_endpoint" => http_endpoint_name} = consumer_attrs,
         databases,
         http_endpoints
       ) do
    # Find the sequence and its associated database
    sequence =
      Enum.find_value(databases, fn database ->
        Enum.find(database.sequences, &(&1.name == sequence_name))
      end)

    unless sequence do
      raise "Sequence '#{sequence_name}' not found for webhook subscription '#{name}'"
    end

    database = Sequin.Enum.find!(databases, fn db -> db.id == sequence.postgres_database_id end)
    table = Sequin.Enum.find!(database.tables, &(&1.oid == sequence.table_oid))
    table = %{table | sort_column_attnum: sequence.sort_column_attnum}

    http_endpoint =
      Enum.find(http_endpoints, fn endpoint -> endpoint.name == http_endpoint_name end)

    unless http_endpoint do
      raise "HTTP endpoint '#{http_endpoint_name}' not found for webhook subscription '#{name}'"
    end

    record_consumer_state = build_record_consumer_state(consumer_attrs["consumer_start"], table, sequence)

    %{
      name: name,
      status: parse_status(consumer_attrs["status"]),
      size: Map.get(consumer_attrs, "batch_size", 1),
      sequence_id: sequence.id,
      replication_slot_id: database.replication_slot.id,
      http_endpoint_id: http_endpoint.id,
      record_consumer_state: record_consumer_state,
      sequence_filter: %{
        actions: ["insert", "update", "delete"],
        group_column_attnums: group_column_attnums(consumer_attrs["group_column_attnums"], table),
        column_filters: column_filters(consumer_attrs["filters"], table)
      }
    }
  end

  defp build_record_consumer_state(nil, table, sequence) do
    # Default to beginning if not specified
    build_record_consumer_state(%{"position" => "beginning"}, table, sequence)
  end

  defp build_record_consumer_state(%{"position" => position} = start_config, table, sequence) do
    producer = "table_and_wal"

    initial_min_cursor =
      case position do
        "beginning" ->
          sequence.sort_column_attnum && KeysetCursor.min_cursor(table)

        "end" ->
          nil

        "from" ->
          value = start_config["value"]
          unless value, do: raise("Missing 'value' for consumer_start position 'from'")
          KeysetCursor.min_cursor(table, value)

        invalid ->
          raise "Invalid consumer_start position '#{invalid}'. Must be 'beginning', 'end', or 'from'"
      end

    %{
      "producer" => producer,
      "initial_min_cursor" => initial_min_cursor
    }
  end

  defp group_column_attnums(nil, %PostgresDatabaseTable{} = table) do
    PostgresDatabaseTable.default_group_column_attnums(table)
  end

  defp group_column_attnums(attnums, _table) when is_list(attnums) do
    Enum.map(attnums, &String.to_integer/1)
  end

  defp parse_status(nil), do: :active
  defp parse_status("active"), do: :active
  defp parse_status("disabled"), do: :disabled

  defp parse_status(invalid_status) do
    raise "Invalid status '#{invalid_status}' for webhook subscription. Must be either 'active' or 'disabled'"
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

  ########################
  ## HTTP Pull Consumers ##
  ########################

  defp upsert_http_pull_consumers!(account_id, %{"consumer_groups" => consumers}, databases) do
    Logger.info("Creating HTTP pull consumers: #{inspect(consumers, pretty: true)}")
    Enum.map(consumers, &upsert_http_pull_consumer!(account_id, &1, databases))
  end

  defp upsert_http_pull_consumers!(_account_id, %{}, _databases), do: []

  defp upsert_http_pull_consumer!(account_id, %{"name" => name} = consumer_attrs, databases) do
    case Sequin.Consumers.find_http_pull_consumer(account_id, name: name) do
      {:ok, existing_consumer} ->
        params = parse_http_pull_consumer_params(consumer_attrs, databases)

        case Sequin.Consumers.update_consumer_with_lifecycle(existing_consumer, params) do
          {:ok, consumer} ->
            Logger.info("Updated HTTP pull consumer: #{inspect(consumer, pretty: true)}")
            consumer

          {:error, error} when is_exception(error) ->
            raise "Failed to update HTTP pull consumer: #{Exception.message(error)}"

          {:error, %Ecto.Changeset{} = changeset} ->
            raise "Failed to update HTTP pull consumer: #{inspect(changeset)}"
        end

      {:error, %NotFoundError{}} ->
        params = parse_http_pull_consumer_params(consumer_attrs, databases)

        case Sequin.Consumers.create_http_pull_consumer_for_account_with_lifecycle(account_id, params) do
          {:ok, consumer} ->
            Logger.info("Created HTTP pull consumer: #{inspect(consumer, pretty: true)}")
            consumer

          {:error, error} when is_exception(error) ->
            raise "Failed to create HTTP pull consumer: #{Exception.message(error)}"

          {:error, %Ecto.Changeset{} = changeset} ->
            raise "Failed to create HTTP pull consumer: #{inspect(changeset)}"
        end
    end
  end

  defp parse_http_pull_consumer_params(%{"name" => name, "sequence" => sequence_name} = consumer_attrs, databases) do
    # Find the sequence and its associated database
    sequence =
      Enum.find_value(databases, fn database ->
        Enum.find(database.sequences, &(&1.name == sequence_name))
      end)

    unless sequence do
      raise "Sequence '#{sequence_name}' not found for consumer group '#{name}'"
    end

    database = Sequin.Enum.find!(databases, fn db -> db.id == sequence.postgres_database_id end)
    table = Sequin.Enum.find!(database.tables, &(&1.oid == sequence.table_oid))
    table = %{table | sort_column_attnum: sequence.sort_column_attnum}

    record_consumer_state = build_record_consumer_state(consumer_attrs["consumer_start"], table, sequence)

    %{
      name: name,
      status: parse_status(consumer_attrs["status"]),
      sequence_id: sequence.id,
      max_ack_pending: Map.get(consumer_attrs, "max_ack_pending", 100),
      replication_slot_id: database.replication_slot.id,
      record_consumer_state: record_consumer_state,
      sequence_filter: %{
        actions: ["insert", "update", "delete"],
        group_column_attnums: group_column_attnums(consumer_attrs["group_column_attnums"], table),
        column_filters: column_filters(consumer_attrs["filters"], table)
      }
    }
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
end
