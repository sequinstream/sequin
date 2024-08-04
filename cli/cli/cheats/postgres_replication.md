// List available databases
sequin databases ls

// Add a new postgres replication (interactive)
sequin sources postgres add

// Add a new postgres replication with an existing database
sequin sources postgres add --database-name <database_name>

// List postgres replications
sequin sources postgres ls

// Show postgres replication info
sequin sources postgres info [id]

// Remove a postgres replication
sequin sources postgres rm [id]

// Edit a postgres replication
sequin sources postgres edit [id]
