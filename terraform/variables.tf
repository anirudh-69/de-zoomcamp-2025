variable "credentials" {
    description = "My Credentials"
    default = "./keys/my-creds.json"
}

variable "location" {
  description = "Project Location"
  default     = "asia-south2"
}

variable project {
  description = "Project"
  default     = "dtc-de-449114"
}

variable "region" {
  description = "Region"
  default     = "asia-south2"
  
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "dtc-de-449114-terra-bucket"
}