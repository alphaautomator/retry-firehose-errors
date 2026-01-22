# Step Function Trigger Tool

A TypeScript tool that triggers AWS Step Functions for every 5-minute interval across a date range.

## Overview

This tool generates executions for a date range with 5-minute intervals throughout each day (288 executions per day).

## Configuration

Add to your `.env` file:

```bash
# Step Function State Machine ARN
STATE_MACHINE_ARN=arn:aws:states:us-east-1:123456789012:stateMachine:your-state-machine

# Date range for processing (YYYY-MM-DD format)
START_DATE=2025-12-25
END_DATE=2026-01-11

# AWS Region
AWS_REGION=us-east-1
```

## Usage

```bash
npm run trigger
```

The tool uses `START_DATE` and `END_DATE` from your `.env` file to determine the date range.

## Payload Format

Each Step Function execution receives:

```json
{
  "time": "2026-01-22T08:05:00.000Z"
}
```

## Time Slots

The tool generates executions for each day in the range:
- **00:00:00** to **23:55:00** in 5-minute intervals
- **288 executions per day**
- If date range is 3 days, total would be **864 executions**

Example time slots for a date range:
```
2025-12-25T00:00:00.000Z
2025-12-25T00:05:00.000Z
2025-12-25T00:10:00.000Z
...
2025-12-25T23:55:00.000Z
2025-12-26T00:00:00.000Z
2025-12-26T00:05:00.000Z
...
2026-01-11T23:55:00.000Z
```

## Execution Names

Each execution has a unique name based on timestamp:
```
retry-execution-2025-12-25T08-05-00-000
retry-execution-2025-12-25T08-10-00-000
...
retry-execution-2026-01-11T23-55-00-000
```

## Redrive for Failed Executions

If an execution already exists and failed, the tool uses the **RedriveExecution API** to retry it:
- ✅ Same execution ARN (no new execution created)
- ✅ Retries from point of failure
- ✅ Preserves execution history
- ✅ More efficient than creating new executions

## Sample Output

```
Initialized with:
  State Machine ARN: arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine
  Date Range: 2025-12-25 to 2026-01-11
  Days: 18
  Interval: 5 minutes
  Total executions: 5184 (288 per day)
  Concurrency: 20

🚀 Starting Step Function trigger process...

Triggering 5184 executions with concurrency 20...

  [1/5184] Triggering: 2025-12-25T00:00:00.000Z
  [2/5184] Triggering: 2025-12-25T00:05:00.000Z
  [3/5184] Triggering: 2025-12-25T00:10:00.000Z
  ... (20 at a time)
    ✓ Success
    ✓ Success
    ✓ Success

======================================================================
📊 Summary
======================================================================
Total executions: 5184
Successful: 5184
Failed: 0
======================================================================

✓ Process completed!
```

## AWS Permissions Required

```json
{
  "Effect": "Allow",
  "Action": [
    "states:StartExecution",
    "states:DescribeExecution",
    "states:RedriveExecution"
  ],
  "Resource": [
    "arn:aws:states:region:account-id:stateMachine:your-state-machine",
    "arn:aws:states:region:account-id:execution:your-state-machine:*"
  ]
}
```

## Features

- ✅ Processes date ranges (multiple days)
- ✅ Generates 288 time slots per day (5-minute intervals for 24 hours)
- ✅ **Parallel execution with configurable concurrency** for faster processing
- ✅ Unique execution names to prevent duplicates
- ✅ **Smart retry logic using RedriveExecution**: Checks execution status before triggering
  - Skips if already succeeded or running
  - **Uses redrive API** to retry failed executions (same execution ARN)
  - Creates new execution if doesn't exist
- ✅ Progress tracking with real-time feedback
- ✅ Comprehensive error handling and reporting
- ✅ Detailed summary with new/redriven/skipped/failed counts

## Error Handling

- Failed executions are tracked and reported
- Process continues even if individual executions fail
- Detailed error messages for troubleshooting
- Summary shows all failed executions at the end

## Concurrency Control

The tool processes 20 executions in parallel using Bluebird's concurrency control. This provides natural throttling while maximizing throughput. If you encounter rate limits, you can reduce the concurrency value in the code.

## Troubleshooting

**Authentication errors**: Ensure AWS credentials are properly configured

**Execution already exists**: The tool checks the existing execution's status:
- **SUCCEEDED or RUNNING**: Skips (no duplicate execution)
- **FAILED/TIMED_OUT/ABORTED**: Uses RedriveExecution API to retry (same ARN)

**Throttling errors**: Increase the delay between executions if you hit rate limits

**Invalid State Machine ARN**: Verify the ARN in your `.env` file is correct
