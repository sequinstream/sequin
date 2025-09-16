resource "aws_vpc" "sequin-main" {
  cidr_block                       = var.vpc_cidr_block
  enable_dns_hostnames             = true
  enable_dns_support               = true
  assign_generated_ipv6_cidr_block = true

  tags = {
    Name = "sequin-main-vpc"
  }
}

resource "aws_subnet" "sequin-public-primary" {
  vpc_id                          = aws_vpc.sequin-main.id
  cidr_block                      = cidrsubnet(aws_vpc.sequin-main.cidr_block, 8, 1)
  availability_zone               = var.primary_availability_zone
  map_public_ip_on_launch         = true
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sequin-main.ipv6_cidr_block, 8, 1)
  assign_ipv6_address_on_creation = true

  tags = {
    Name = "sequin-public-${var.primary_availability_zone}"
  }
}

resource "aws_subnet" "sequin-public-secondary" {
  vpc_id                          = aws_vpc.sequin-main.id
  cidr_block                      = cidrsubnet(aws_vpc.sequin-main.cidr_block, 8, 2)
  availability_zone               = var.secondary_availability_zone
  map_public_ip_on_launch         = true
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sequin-main.ipv6_cidr_block, 8, 2)
  assign_ipv6_address_on_creation = true

  tags = {
    Name = "sequin-public-${var.secondary_availability_zone}"
  }
}

resource "aws_subnet" "sequin-private-primary" {
  vpc_id                          = aws_vpc.sequin-main.id
  cidr_block                      = cidrsubnet(aws_vpc.sequin-main.cidr_block, 8, 3)
  availability_zone               = var.primary_availability_zone
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sequin-main.ipv6_cidr_block, 8, 3)
  assign_ipv6_address_on_creation = true

  tags = {
    Name = "sequin-private-${var.primary_availability_zone}"
  }
}

resource "aws_subnet" "sequin-private-secondary" {
  vpc_id                          = aws_vpc.sequin-main.id
  cidr_block                      = cidrsubnet(aws_vpc.sequin-main.cidr_block, 8, 4)
  availability_zone               = var.secondary_availability_zone
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sequin-main.ipv6_cidr_block, 8, 4)
  assign_ipv6_address_on_creation = true

  tags = {
    Name = "sequin-private-${var.secondary_availability_zone}"
  }
}

resource "aws_internet_gateway" "sequin-main" {
  vpc_id = aws_vpc.sequin-main.id

  tags = {
    Name = "sequin-main-igw"
  }
}

# Elastic IP for NAT Gateway
resource "aws_eip" "sequin-nat" {
  tags = {
    Name = "sequin-nat-eip"
  }

  depends_on = [aws_internet_gateway.sequin-main]
}

# NAT Gateway for private subnet internet access
resource "aws_nat_gateway" "sequin-main" {
  allocation_id = aws_eip.sequin-nat.id
  subnet_id     = aws_subnet.sequin-public-primary.id

  tags = {
    Name = "sequin-main-nat"
  }

  depends_on = [aws_internet_gateway.sequin-main]
}

resource "aws_route_table" "sequin-public" {
  vpc_id = aws_vpc.sequin-main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.sequin-main.id
  }

  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.sequin-main.id
  }

  tags = {
    Name = "sequin-public-rt"
  }
}

resource "aws_route_table_association" "sequin-public-primary" {
  subnet_id      = aws_subnet.sequin-public-primary.id
  route_table_id = aws_route_table.sequin-public.id
}

resource "aws_route_table_association" "sequin-public-secondary" {
  subnet_id      = aws_subnet.sequin-public-secondary.id
  route_table_id = aws_route_table.sequin-public.id
}

resource "aws_route_table" "sequin-private" {
  vpc_id = aws_vpc.sequin-main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.sequin-main.id
  }

  route {
    ipv6_cidr_block        = "::/0"
    egress_only_gateway_id = aws_egress_only_internet_gateway.sequin-main.id
  }

  tags = {
    Name = "sequin-private-rt"
  }
}

resource "aws_route_table_association" "sequin-private-primary" {
  subnet_id      = aws_subnet.sequin-private-primary.id
  route_table_id = aws_route_table.sequin-private.id
}

resource "aws_route_table_association" "sequin-private-secondary" {
  subnet_id      = aws_subnet.sequin-private-secondary.id
  route_table_id = aws_route_table.sequin-private.id
}

resource "aws_egress_only_internet_gateway" "sequin-main" {
  vpc_id = aws_vpc.sequin-main.id

  tags = {
    Name = "sequin-main-eigw"
  }
}
