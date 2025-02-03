variable "location" {
  description = "Project Location"
  default = "US"
}

variable "region" {
  description = "Project Region"
  default = "us-central1"
}

variable "zone" {
  description = "Project Zone"
  default = "us-central1-c"
}

variable "bq_dataset_name" {
  description = "Big Query Dataset Name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "terraform-demo-448011-terra-bucket"
  default = "STANDARD"
}

variable "gcs_storage_class" {
  description = "GCS Storage Class"
  default = "STANDARD"
}

variable "creds" {
  description = "My Credentitals"
  default = "./keys/terraform-key.json"
}