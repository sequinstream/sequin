
```
create table customers (
    id serial primary key,
    first_name text not null,
    last_name text not null,
    email text not null unique,
    address text,
    city text,
    state text,
    postal_code text,
    country text,
    created_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now()
);
```

```
INSERT INTO public.customers (id, first_name, last_name, email, address, city, state, postal_code, country, created_at, updated_at) VALUES
(1, 'John', 'Johnson', 'john.smith@example.com', '123 Main St', 'Boston', 'MA', '02108', 'USA', '2025-04-17 17:31:44.35438-07', '2025-04-17 17:31:44.35438-07'),
(3, 'Michael', 'Johnson', 'mbrown@example.com', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA', '2025-04-17 17:31:44.35438-07', '2025-04-17 17:31:44.35438-07'),
(4, 'Sarah', 'Johnson', 'swilson@example.com', '321 Maple Dr', 'New York', 'NY', '10001', 'USA', '2025-04-17 17:31:44.35438-07', '2025-04-17 17:31:44.35438-07'),
(5, 'David', 'Johnson', 'dlee@example.com', '654 Cedar Ln', 'Seattle', 'WA', '98101', 'USA', '2025-04-17 17:31:44.35438-07', '2025-04-17 17:31:44.35438-07'),
(2, 'Emily', 'Johnson', 'emily.j@example.com', '456 Oak Ave', 'San Francisco', 'CA', '94102', 'USA', '2025-04-17 17:31:44.35438-07', '2025-04-17 17:31:44.35438-07');
```


# Populate typesense collections
```elixir

client = Sequin.Sinks.Typesense.Client.new([
  url: "http://localhost:8108",
  api_key: "xyz"
])



# Define the schema for the Customers collection
customer_schema = %{
  "name" => "customers",
  "fields" => [
    %{"name" => "first_name", "type" => "string"},
    %{"name" => "last_name", "type" => "string", "sort" => true},
    %{"name" => "email", "type" => "string", "infix" => true},
    %{"name" => "address", "type" => "string"},
    %{"name" => "city", "type" => "string"},
    %{"name" => "state", "type" => "string"},
    %{"name" => "postal_code", "type" => "string"},
    %{"name" => "country", "type" => "string"}
  ],
  "default_sorting_field" => "last_name"
}
Sequin.Sinks.Typesense.Client.create_collection(client, customer_schema)


# Define the schema for the Products collection
product_schema = %{
  "name" => "products",
  "fields" => [
    %{"name" => "name", "type" => "string"},
    %{"name" => "description", "type" => "string"},
    %{"name" => "price", "type" => "float"},
    %{"name" => "stock_quantity", "type" => "int32"},
    %{"name" => "category_id", "type" => "int32"},
    %{"name" => "image_url", "type" => "string", "index" => false},
    %{"name" => "is_featured", "type" => "bool"},
    %{"name" => "is_active", "type" => "bool"}
  ],
  "default_sorting_field" => "price"
}
Sequin.Sinks.Typesense.Client.create_collection(client, product_schema)




```


# Do a query

```elixir

go = fn me -> 
  IO.puts([IO.ANSI.clear, IO.ANSI.home, "\n", to_string(DateTime.now!("Etc/UTC"))])
  Req.get!("http://localhost:8108/collections/customers/documents/search",
    headers: [{"X-TYPESENSE-API-KEY", "xyz"}],
    params: %{q: "johns",
              #  ^^^^^^^ TYPESENSE SEARCH STRING <<<<<<<<<<<<<<
              query_by: "first_name,last_name,email"
    })
    |> case do
         %{status: 200, body: %{"hits" => hs}} ->
           for %{"highlights" => hlt} <- hs do
             IO.inspect(hlt, label: "highlight")
           end
           :ok
         e -> dbg(e)
       end
    
    Process.sleep(333)
    me.(me)
end
# go.(fn _ -> :ok end)
watcher = spawn(fn -> go.(go) end)


```
