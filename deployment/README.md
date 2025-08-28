# Sequin AWS Deployment

This repository contains Terraform infrastructure code to deploy [Sequin](https://github.com/sequinstream/sequin) on AWS.

**Sequin** is a tool for capturing database changes and streaming them to external systems like Kafka, SQS, Redis, and webhooks. It provides a simple way to build event-driven architectures and keep external systems in sync with your database.

## Deployment Options

Two deployment configurations are available:

- **`terraform-ecs-ec2/`**: Traditional ECS deployment using EC2 instances
- **`terraform-ecs-fargate/`**: Serverless ECS deployment using AWS Fargate

## Project Layout

Each deployment configuration is organized into two separate Terraform directories:

### `infra/` - Core infrastructure

Contains shared infrastructure resources that are deployed once:
- **VPC & networking**: VPC, subnets, NAT Gateway, Internet Gateway
- **ECS cluster**: Container orchestration cluster
- **RDS database**: Postgres database for Sequin
- **Redis/ElastiCache**: Redis instance for caching and queuing
- **Load balancer**: Application Load Balancer with SSL support
- **Security groups & IAM**: Security and access control
- **Bastion host**: EC2 instance for secure database access (EC2 deployment only)

### `app/` - Application deployment

Contains the Sequin application deployment:
- **ECS service**: Sequin container service configuration
- **Task definition**: Container specifications and environment variables
- **Secrets management**: Auto-generated secrets and configuration
- **CloudWatch logs**: Application logging setup

This separation allows you to manage infrastructure changes independently from application deployments.

## Usage

### Prerequisites
- AWS CLI configured with appropriate permissions
- Terraform installed (>= 1.0)
- An AWS key pair for EC2 access (EC2 deployment only)

### 1. Initialize Backend Configuration

Run the initialization script to set up Terraform state management:

```bash
make init-terraform
```

This command will:
- Prompt you to choose between EC2 or Fargate deployment
- Prompt you for S3 bucket name and AWS region during setup
- Optionally create the specified S3 bucket in AWS if it doesn't exist
- Create `[selected-deployment]/infra/backend.tfbackend` with S3 backend configuration for infrastructure state
- Create `[selected-deployment]/app/backend.tfbackend` with S3 backend configuration for application state
- Create `[selected-deployment]/app/remote-state.auto.tfvars` with variables to reference the infrastructure state

### 2. Deploy Core Infrastructure

Navigate to the infrastructure directory of your chosen deployment and deploy the foundational resources:

```bash
cd terraform-ecs-fargate/infra/  # or terraform-ecs-ec2/infra/
terraform init -backend-config=backend.tfbackend
terraform apply
```

You'll be prompted to provide:

- **EC2 key name**: Name of your AWS key pair for SSH access (EC2 deployment only)
- **Database password**: Secure password for the Sequin config Postgres database

This will create all the networking, database, and cluster infrastructure needed for Sequin.

### 3. Deploy Sequin application

Navigate to the application directory and deploy Sequin:

```bash
cd ../app/
terraform init -backend-config=backend.tfbackend
terraform apply -var image_tag=latest
```

This will:
- Deploy the Sequin container to your ECS cluster
- Auto-generate secure secrets (encryption keys, admin password)
- Configure database and Redis connections
- Set up load balancer routing and health checks

### 4. Access your deployment

After both deployments complete, you can access Sequin at the load balancer URL displayed in the outputs.

Note that the Sequin setup process creates a user with a default username and password:

- email: `admin@sequinstream.com`
- Password: `sequinpassword!`

**You should change the password immediately after logging in.**

### 5. Update secrets (optional)

`app` creates secrets in AWS Secrets Manager. Placeholders are used for many optional secrets, such as GitHub credentials (for GitHub OAuth). You can manage these secrets using the AWS Secrets Manager console or CLI.

See [Configuration](https://sequinstream.com/docs/reference/configuration) for the full list of Sequin configuration options.

## Configuration

### Variables

- **Image tag**: Specify a Sequin version with `-var image_tag=v0.13.0` or use `latest`
- **Instance sizes**: Modify `variables.tf` to adjust compute resources:
  - EC2 deployment: Adjust EC2 and RDS instance types
  - Fargate deployment: Adjust CPU/memory allocation and RDS instance types

### SSL/HTTPS

To enable HTTPS, provide an SSL certificate ARN in the `ssl_certificate_arn` variable.
