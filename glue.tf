# Author: Murugesan Alagusundaram
# Department: Data Engineering @ DSE Office

resource "aws_glue_catalog_database" "de" {
  name  = "${var.glue_catalog_db_name}"
}


resource "aws_glue_crawler" "heap_lpw" {
  database_name = "${aws_glue_catalog_database.datalake_raw.name}"
  name          = "${var.glue_datalake_crawler_name_prefix}-heap-lpw"
  role          = "${lookup(var.glue_service_role_arn, terraform.workspace)}"
  table_prefix  = "heap_lpw_"

  s3_target {
    path = "s3://${lookup(var.heap_dest_bucket, terraform.workspace)}/heap/lpw"
    exclusions  = ["**/_MANIFEST"]
  }

  schedule = "cron(0 12 ? * * *)"  #4AM PDT

  configuration = <<EOF
{
  "Version":1.0,
  "Grouping": {
    "TableGroupingPolicy": "CombineCompatibleSchemas"
  },
  "CrawlerOutput": {
    "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" },
    "Tables": { "AddOrUpdateBehavior": "MergeNewColumns" }
  }
}
EOF

}

resource "aws_glue_crawler" "datalake_raw_official" {
  #count = "${lookup(var.deploy_prod_only, terraform.workspace)}"
  database_name = "${aws_glue_catalog_database.datalake_raw.name}"
  name          = "${var.glue_datalake_crawler_name_prefix}-raw"
  role          = "${lookup(var.glue_service_role_arn, terraform.workspace)}"

  s3_target {
    path = "s3://${lookup(var.heap_dest_bucket, terraform.workspace)}/data-quality"
  }

  s3_target {
    path = "s3://${lookup(var.heap_dest_bucket, terraform.workspace)}/enrollments"
  }

  s3_target {
    path = "s3://${lookup(var.heap_dest_bucket, terraform.workspace)}/fees"
  }

  s3_target {
    path = "s3://${lookup(var.heap_dest_bucket, terraform.workspace)}/rnl"
  }

  configuration = <<EOF
{
  "Version":1.0,
  "Grouping": {
    "TableGroupingPolicy": "CombineCompatibleSchemas"
  }
}
EOF

}
