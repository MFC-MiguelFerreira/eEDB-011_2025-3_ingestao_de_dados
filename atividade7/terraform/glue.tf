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
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "OpenCSVSerDe"
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        "separatorChar" = "\t"
        "quoteChar"     = "\""
        "escapeChar"    = "\\"
      }
    }

    columns {
      name = "Segmento"
      type = "string"
    }
    columns {
      name = "CNPJ"
      type = "string"
    }
    columns {
      name = "Nome"
      type = "string"
    }
  }
}
