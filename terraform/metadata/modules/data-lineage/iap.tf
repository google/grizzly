resource "google_iap_brand" "grizzly_brand" {
  support_email     = var.iap_support_email
  application_title = "Cloud IAP protected application"
  project           = var.gcp_project_id
}



