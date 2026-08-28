variable "region" {
  description = "Região AWS"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Nome do cluster EKS e prefixo dos recursos"
  type        = string
  default     = "order-saga"
}

variable "environment" {
  description = "Ambiente"
  type        = string
  default     = "prod"
}

variable "vpc_cidr" {
  description = "CIDR da VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# --- Node group ---
variable "node_instance_types" {
  description = "Tipos de instância do node group"
  type        = list(string)
  default     = ["t3.small"]
}

variable "node_desired_size" {
  type    = number
  default = 4
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 5
}

variable "node_capacity_type" {
  description = "ON_DEMAND ou SPOT (SPOT reduz custo)"
  type        = string
  default     = "SPOT"
}

# --- RDS ---
variable "db_username" {
  description = "Usuário do RDS Postgres"
  type        = string
  default     = "saga"
}

variable "db_password" {
  description = "Senha do RDS Postgres (informar via tfvars/ambiente - NUNCA no git)"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_allocated_storage" {
  type    = number
  default = 20
}
