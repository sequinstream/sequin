// Create a tunnel to expose a local port
sequin tunnel --ports=5432:my-database-id

// Create multiple tunnels
sequin tunnel --ports=5432:db-id,6379:redis-id

// Use key ID and key secret inline
sequin tunnel --key-id=my-key --key-secret=my-secret --ports=8080:service-id

// Use a specific context
sequin --context=dev tunnel --ports=8080:service-id
