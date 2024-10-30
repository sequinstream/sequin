terraform {
  required_providers {
    sequin = {
      source = "hashicorp.com/edu/sequin"
    }
  }
}

provider "sequin" {
  endpoint = "http://localhost:4000"
  api_key = "ipVepZy6KV-dtVrhiAZ6tb--I8PoUmzOHj6aro22JCemGY4yp9ByQ7Ys7txTf7DJ"
}

data "sequin_database" "example" {}

output "databases" {
  value = data.sequin_database.example
}

