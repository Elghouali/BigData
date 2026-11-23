output "bucket_name" {
  description = "Optional AWS S3 bucket name."
  value       = aws_s3_bucket.lab.bucket
}

output "bucket_arn" {
  description = "Optional AWS S3 bucket ARN."
  value       = aws_s3_bucket.lab.arn
}
