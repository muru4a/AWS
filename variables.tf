variable heap_dest_bucket {
     default = "com-fngn-finr-dataeng-sparkmuru"
}

variable heap_src_bucket {
     default = "com-fngn-finr-dataeng-heap"
}

variable "aws_region" {

    default = "us-west-1"

}

variable "aws_profile" {

    default = "default"

}

variable "glue_catalog_db_name" {
  default = "fngn-dataeng-glue-datalake"
}

variable "glue_catalog_raw_db_name" {
  default = "fngn-dataeng-datalake-raw"
}


#TBD: eventually will use datalake raw data instead

variable "glue_crawler_name_prefix" {
  default = "fngn-dataeng"
}

variable "glue_datalake_crawler_name_prefix" {
  default = "fngn-dataeng-datalake"
}

variable "glue_service_role_arn" {
  type = "map"
}
