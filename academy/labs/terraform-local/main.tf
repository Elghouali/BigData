terraform {
  required_version = ">= 1.6.0"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

resource "local_file" "academy" {
  filename = "${path.module}/academy-output.txt"
  content  = var.message
}
