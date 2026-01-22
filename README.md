# AWS Automation Tools

Two TypeScript tools for AWS automation:
1. **Retry Firehose Errors** - Retry failed Firehose records from S3
2. **Step Function Trigger** - Trigger Step Functions in 5-minute intervals for a full day

## Features

- ✅ Fetches S3 objects within a specific date range (configurable)
- ✅ **Parallel processing with concurrency of 20** for all operations
- ✅ Decodes base64-encoded data from S3 error records
- ✅ **Processes ALL events** to prevent data loss
- ✅ Sends all records back to Firehose in batches (up to 500 per batch)
- ✅ **Automatically deletes S3 files after successful Firehose delivery**
- ✅ Detailed logging with progress tracking

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
# === Firehose Retry Tool ===
S3_BUCKET=your-bucket-name
S3_PREFIX=your-prefix-path/
FIREHOSE_ARN=arn:aws:firehose:us-east-1:123456789012:deliverystream/your-stream

# Date range for processing (YYYY-MM-DD format)
START_DATE=2025-12-25
END_DATE=2026-01-11

# === Step Function Trigger Tool ===
STATE_MACHINE_ARN=arn:aws:states:us-east-1:123456789012:stateMachine:your-state-machine
TARGET_DATE=2026-01-22

# === Common ===
AWS_REGION=us-east-1
```

**Note**: The tool processes ALL events from error files to ensure no data is lost when deleting S3 files.

## Usage

### Tool 1: Retry Firehose Errors

```bash
# Development mode
npm run dev

# Production build
npm run build
npm start
```

### Tool 2: Trigger Step Functions

```bash
# Trigger for date range from .env (START_DATE to END_DATE)
npm run trigger
```

See [STEP_FUNCTION_TRIGGER.md](./STEP_FUNCTION_TRIGGER.md) for detailed documentation.

## How It Works

1. **Read Configuration**: Loads S3 bucket, Firehose ARN, and date range from `.env` file
2. **Generate Date-Hour Tasks**: Creates S3 prefixes for each date-hour combination (YYYY/MM/DD/HH format)
   - Example: `your-prefix/2025/12/25/00/`, `your-prefix/2025/12/25/01/`, etc.
3. **List S3 Objects (Parallel)**: Fetches objects from each date-hour prefix with **concurrency of 20**
4. **Download & Decode (Parallel)**: Downloads and decodes all S3 error files with **concurrency of 20**
5. **Parse Records**: Parses JSON records from the decoded data
6. **Send to Firehose (Parallel)**: Processes files with **concurrency of 20**:
   - Sends filtered records to Firehose (batches of 500)
   - On success, deletes the S3 file
   - On failure, keeps the S3 file for retry

## S3 Path Structure

The tool expects S3 objects to be organized by date and hour:
```
s3://bucket-name/prefix/YYYY/MM/DD/HH/error-file.json
```

Example:
```
s3://my-bucket/firehose-errors/2025/12/25/00/error1.json
s3://my-bucket/firehose-errors/2025/12/25/00/error2.json
s3://my-bucket/firehose-errors/2025/12/25/01/error1.json
s3://my-bucket/firehose-errors/2025/12/26/15/error1.json
```

The tool will automatically construct these paths based on your date range and process them in parallel with a concurrency of 20.

## AWS Permissions Required

### S3 Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:ListBucket",
    "s3:GetObject",
    "s3:DeleteObject"
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
