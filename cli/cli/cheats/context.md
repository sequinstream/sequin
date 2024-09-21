// Add a context
sequin-cli context add dev --hostname=localhost:7376 --set-default
sequin-cli context add prod --hostname=sequin.io --tls

// List contexts
sequin-cli context ls

// Use a context
sequin-cli --context=dev stream ls

// Set default context
sequin-cli context select dev

// Select default context from a list
sequin-cli context select
