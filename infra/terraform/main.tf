resource "aws_s3_bucket" "bronze" {
  bucket = "bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "gold"
}