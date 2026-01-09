# Retry Firehose Errors

A TypeScript tool to retry failed Firehose records from S3 by filtering and reprocessing `appointmentPatched` events.

## Features

- ✅ Recursively fetches all S3 objects from the last 5 days
- ✅ Decodes base64-encoded data from S3
- ✅ Filters records by `eventName === "appointmentPatched"`
- ✅ Sends filtered records back to Firehose in batches
- ✅ Handles large datasets with batch processing (500 records per batch)

## Prerequisites

- Node.js (v18 or higher recommended)
- AWS credentials configured (via AWS CLI, environment variables, or IAM role)
- Access to the S3 bucket containing error files
- Permissions to write to the target Firehose delivery stream

## Installation

```bash
npm install
```

## Configuration

1. Create a `.env` file in the project root with your configuration:
```
S3_BUCKET=your-bucket-name
S3_PREFIX=your-prefix-path/
FIREHOSE_ARN=arn:aws:firehose:us-east-1:123456789012:deliverystream/your-stream
AWS_REGION=us-east-1
```

## Usage

### Development Mode

```bash
npm run dev
```

### Production Build

```bash
npm run build
npm start
```

## How It Works

1. **Read Configuration**: Loads S3 bucket and prefix from `.env` file
2. **List S3 Objects**: Recursively lists all objects modified in the last 5 days
3. **Download & Decode**: Downloads each object and decodes from base64
4. **Parse Records**: Parses JSON records (supports newline-delimited JSON)
5. **Filter**: Keeps only records where `eventName === "appointmentPatched"`
6. **Send to Firehose**: Sends filtered records back to Firehose in batches of 500

## AWS Permissions Required

### S3 Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:ListBucket",
    "s3:GetObject"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket-name",
    "arn:aws:s3:::your-bucket-name/*"
  ]
}
```

### Firehose Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "firehose:PutRecordBatch"
  ],
  "Resource": [
    "arn:aws:firehose:region:account-id:deliverystream/your-stream-name"
  ]
}
```

## Error Handling

- Invalid S3 objects are logged and skipped
- Failed JSON parsing is logged with the first 100 characters of the line
- Firehose batch failures are tracked and reported
- Process continues even if individual records fail

## Output

The tool provides detailed logging:
- Number of S3 objects found
- Processing progress for each file
- Total records parsed and filtered
- Batch processing status
- Final success/failure counts

## Troubleshooting

**No objects found**: Check your S3_BUCKET and S3_PREFIX in `.env`

**Authentication errors**: Ensure AWS credentials are properly configured

**Parsing errors**: Check that S3 objects contain valid base64-encoded JSON

**Firehose errors**: Verify the Firehose ARN and delivery stream permissions
