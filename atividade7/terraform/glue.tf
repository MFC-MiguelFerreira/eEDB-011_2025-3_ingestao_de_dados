resource "aws_glue_catalog_database" "atividade7" {
  name        = "atividade7"
  description = "Database to store atividade7 data"
}

resource "aws_glue_catalog_table" "bancos" {
  name          = "bancos"
  database_name = aws_glue_catalog_database.atividade7.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.atividade5_bucket.bucket}/bancos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "segment"
      type = "string"
    }
    columns {
      name = "cnpj"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
  }
}
