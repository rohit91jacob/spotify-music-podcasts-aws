output "delivery_stream_arn" {
  description = "Firehose delivery stream ARN."
  value       = aws_kinesis_firehose_delivery_stream.listen_events.arn
}

output "delivery_stream_name" {
  description = "Firehose delivery stream name."
  value       = aws_kinesis_firehose_delivery_stream.listen_events.name
}
