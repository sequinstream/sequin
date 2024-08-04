# Postgres Database Commands

## List Postgres Databases

sequin postgres ls [--as-curl]

## Add a New Postgres Database

sequin postgres add [flags]

Flags:
--database Database name
--hostname Database hostname
--port Database port
--username Database username
--password Database password
--name Friendly name for the database
--ssl Use SSL for connection
--json JSON string containing postgres database configuration
--json-file Path to JSON file containing postgres database configuration
--as-curl Print the curl command equivalent

## Show Postgres Database Information

sequin postgres info [id] [flags]

Flags:
--as-json Print postgres database info as JSON
--as-curl Print the curl command equivalent

## Edit an Existing Postgres Database

sequin postgres edit [id] [flags]

Flags:
--json JSON string containing postgres database configuration
--json-file Path to JSON file containing postgres database configuration
--as-curl Print the curl command equivalent

## Remove a Postgres Database

sequin postgres remove [id] [flags]

Flags:
--force Force removal without confirmation
--as-curl Print the curl command equivalent

Note: The `postgres` command can be aliased as `pg`.
