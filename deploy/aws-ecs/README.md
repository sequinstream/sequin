# AWS ECS Deployment

These templates assume you need to create a VPC, ECS cluster, and security group. If you already have these resources, you can modify the templates to use them instead of creating new ones.

### Secrets

`secret_key_base` should be a random string of **exactly** 64 characters.

`vault_key` should be a random base64 encoded string of **exactly** 32 characters.

You can generate these secrets with `openssl` like so:

```bash
# Generate SECRET_KEY_BASE
openssl rand -base64 64

# Generate VAULT_KEY
openssl rand -base64 32
```
