variable "bucket_name" {
  type        = string
  default     = "capstone-project-${var.env}"
  description = "bucket name"
}

variable "project_id" {
  description = "project id"
}

variable "location" {
  description = "location"
}

variable "region" {
  description = "region"
}
