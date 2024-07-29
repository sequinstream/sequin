// Create a context
sequin-cli context create dev --server-url=http://localhost:7376
sequin-cli context create prod --server-url=https://sequin.io

// List contexts
sequin-cli context ls

// Use a context
sequin-cli --context=dev stream ls

// Set default context
sequin-cli context select dev

// Select default context from a list
sequin-cli context select
