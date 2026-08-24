output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  value = module.eks.cluster_certificate_authority_data
}

output "node_security_group_id" {
  value = module.eks.node_security_group_id
}

output "rds_endpoint" {
  description = "Endpoint do RDS (preencher o Secret do Helm)"
  value       = aws_db_instance.saga.endpoint
}

output "kubeconfig" {
  description = "Comando para gerar o kubeconfig do EKS"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${var.cluster_name}"
}
