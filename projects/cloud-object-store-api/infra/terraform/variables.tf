variable "aws_region" {
  description = "Region for the optional AWS S3 resource."
  type        = string
  default     = "eu-west-3"
}

variable "bucket_name" {
  description = "Globally unique DNS-compatible AWS S3 bucket name."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Use 3-63 lowercase DNS-compatible characters; start and end alphanumeric."
  }
}
