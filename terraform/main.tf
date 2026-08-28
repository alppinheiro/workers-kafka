provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  state = "available"
}

# --- VPC (2 AZs) ---
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = var.cluster_name
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 2)
  private_subnets = [for i in range(2) : cidrsubnet(var.vpc_cidr, 8, i)]
  public_subnets  = [for i in range(2) : cidrsubnet(var.vpc_cidr, 8, i + 100)]

  enable_nat_gateway   = true
  single_nat_gateway   = true # 1 NAT (custo) - suficiente p/ estudo
  enable_dns_hostnames = true

  tags = { Environment = var.environment, Project = "order-saga" }
}

# --- EKS ---
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.31"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # Estudo: acesso público ao API server (proteger com o SG do kubeconfig).
  cluster_endpoint_public_access = true

  # Admin do cluster via Access Entry (o IAM principal que executa o terraform).
  # Sem isso, `kubectl` retorna 401 Unauthorized após criar o cluster.
  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    main = {
      desired_size   = var.node_desired_size
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      instance_types = var.node_instance_types
      capacity_type  = var.node_capacity_type # SPOT reduz custo
    }
  }

  tags = { Environment = var.environment, Project = "order-saga" }
}

# --- EBS CSI driver (necessário para PVCs/Kafka no EKS) ---
# O driver é um addon gerenciado que precisa de uma IAM role dedicada (IRSA).
# Sem ele, o PVC do Kafka fica Pending para sempre.
data "aws_iam_policy_document" "ebs_csi_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "AmazonEKS_EBS_CSI_DriverRole"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_assume.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = module.eks.cluster_name
  addon_name               = "aws-ebs-csi-driver"
  addon_version            = "v1.64.0-eksbuild.1"
  service_account_role_arn = aws_iam_role.ebs_csi.arn
  depends_on               = [aws_iam_role_policy_attachment.ebs_csi]
}


# --- RDS Postgres (free tier db.t4g.micro) ---
resource "aws_db_subnet_group" "saga" {
  name       = "${var.cluster_name}-subnet"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "rds" {
  name        = "${var.cluster_name}-rds"
  description = "Acesso ao RDS a partir dos nodes do EKS"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
}

resource "aws_db_instance" "saga" {
  identifier     = "${var.cluster_name}-db"
  engine         = "postgres"
  engine_version = "16.4"
  instance_class = var.db_instance_class

  allocated_storage      = var.db_allocated_storage
  storage_encrypted      = true
  skip_final_snapshot    = true
  db_subnet_group_name   = aws_db_subnet_group.saga.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  db_name  = "saga"
  username = var.db_username
  password = var.db_password

  tags = { Environment = var.environment, Project = "order-saga" }
}
