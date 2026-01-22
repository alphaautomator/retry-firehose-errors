import { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { FirehoseClient, PutRecordBatchCommand } from '@aws-sdk/client-firehose';
import * as dotenv from 'dotenv';
import { Readable } from 'stream';
import Bluebird from 'bluebird';

// Load environment variables
dotenv.config();

interface S3ErrorRecord {
  rawData: string;
  errorCode: string;
  errorMessage: string;
  attemptsMade: number;
  arrivalTimestamp: number;
  attemptEndingTimestamp: number;
  lambdaARN: string;
}

interface FirehoseRecord {
  eventName?: string;
  [key: string]: any;
}

interface ProcessedFile {
  key: string;
  records: FirehoseRecord[];
}

interface DateHourTask {
  date: Date;
  hour: number;
  prefix: string;
}

class FirehoseErrorRetry {
  private s3Client: S3Client;
  private firehoseClient: FirehoseClient;
  private s3Bucket: string;
  private s3Prefix: string;
  private firehoseArn: string;
  private startDate: Date;
  private endDate: Date;
  private readonly concurrency: number = 50;

  constructor() {
    const region = process.env.AWS_REGION || 'us-east-1';
    
    this.s3Client = new S3Client({ region });
    this.firehoseClient = new FirehoseClient({ region });
    
    this.s3Bucket = process.env.S3_BUCKET || '';
    this.s3Prefix = process.env.S3_PREFIX || '';
    this.firehoseArn = process.env.FIREHOSE_ARN || '';

    // Parse date range
    const startDateStr = process.env.START_DATE || '2025-12-25';
    const endDateStr = process.env.END_DATE || '2026-01-11';
    
    this.startDate = new Date(startDateStr);
    this.startDate.setHours(0, 0, 0, 0);
    
    this.endDate = new Date(endDateStr);
    this.endDate.setHours(23, 59, 59, 999);

    if (!this.s3Bucket) {
      throw new Error('S3_BUCKET is not defined in .env file');
    }
    if (!this.firehoseArn) {
      throw new Error('FIREHOSE_ARN is not defined in .env file');
    }

    console.log(`Initialized with:`);
    console.log(`  S3 Bucket: ${this.s3Bucket}`);
    console.log(`  S3 Prefix: ${this.s3Prefix}`);
    console.log(`  Firehose ARN: ${this.firehoseArn}`);
    console.log(`  Date Range: ${this.startDate.toISOString().split('T')[0]} to ${this.endDate.toISOString().split('T')[0]}`);
    console.log(`  Processing: All events`);
    console.log(`  Concurrency: ${this.concurrency}`);
  }

  /**
   * Generate all dates between start and end date
   */
  private generateDateRange(): Date[] {
    const dates: Date[] = [];
    const currentDate = new Date(this.startDate);

    while (currentDate <= this.endDate) {
      dates.push(new Date(currentDate));
      currentDate.setDate(currentDate.getDate() + 1);
    }

    return dates;
  }

  /**
   * Format date to S3 path (YYYY/MM/DD/HH)
   */
  private formatDateHourToS3Path(date: Date, hour: number): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hourStr = String(hour).padStart(2, '0');
    return `${year}/${month}/${day}/${hourStr}`;
  }

  /**
   * Generate all date-hour combinations for the date range
   */
  private generateDateHourTasks(): DateHourTask[] {
    const tasks: DateHourTask[] = [];
    const dates = this.generateDateRange();

    for (const date of dates) {
      for (let hour = 0; hour < 24; hour++) {
        const dateHourPath = this.formatDateHourToS3Path(date, hour);
        const prefix = this.s3Prefix ? `${this.s3Prefix}${dateHourPath}/` : `${dateHourPath}/`;
        
        tasks.push({
          date: new Date(date),
          hour,
          prefix,
        });
      }
    }

    return tasks;
  }

  /**
   * List S3 objects for a specific date-hour prefix
   */
  private async listS3ObjectsForPrefix(prefix: string): Promise<string[]> {
    const objects: string[] = [];
    let continuationToken: string | undefined;

    do {
      const command = new ListObjectsV2Command({
        Bucket: this.s3Bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      });

      try {
        const response = await this.s3Client.send(command);

        if (response.Contents) {
          for (const obj of response.Contents) {
            if (obj.Key) {
              objects.push(obj.Key);
            }
          }
        }

        continuationToken = response.NextContinuationToken;
      } catch (error) {
        // Silently skip if prefix doesn't exist
        break;
      }
    } while (continuationToken);

    return objects;
  }

  /**
   * Process a single date-hour task (list objects)
   */
  private async processDateHourTask(task: DateHourTask): Promise<string[]> {
    const objects = await this.listS3ObjectsForPrefix(task.prefix);
    
    if (objects.length > 0) {
      console.log(`  ✓ ${task.prefix}: Found ${objects.length} object(s)`);
    }
    
    return objects;
  }

  /**
   * List all S3 objects within the date range using date-hour-based prefixes with parallel processing
   */
  private async listS3Objects(): Promise<string[]> {
    console.log(`\nFetching objects from ${this.startDate.toISOString().split('T')[0]} to ${this.endDate.toISOString().split('T')[0]}...`);
    
    const tasks = this.generateDateHourTasks();
    console.log(`Processing ${tasks.length} date-hour combinations with concurrency ${this.concurrency}...\n`);
    
    // Process tasks in parallel with concurrency limit using Bluebird
    const results = await Bluebird.map(
      tasks,
      (task: DateHourTask) => this.processDateHourTask(task),
      { concurrency: this.concurrency }
    );

    // Flatten results
    const allObjects = results.flat();

    console.log(`\nTotal objects found: ${allObjects.length}`);
    return allObjects;
  }

  /**
   * Convert stream to string
   */
  private async streamToString(stream: Readable): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
  }

  /**
   * Download and decode S3 object
   */
  private async downloadAndDecodeS3Object(key: string): Promise<FirehoseRecord[]> {
    try {
      const command = new GetObjectCommand({
        Bucket: this.s3Bucket,
        Key: key,
      });

      const response = await this.s3Client.send(command);
      
      if (!response.Body) {
        console.warn(`No body in response for key: ${key}`);
        return [];
      }

      // Read the stream
      const bodyString = await this.streamToString(response.Body as Readable);
      
      // Parse JSON (assuming each line is a separate JSON object with rawData field)
      const records: FirehoseRecord[] = [];
      const lines = bodyString.split('\n').filter(line => line.trim());
      
      for (const line of lines) {
        try {
          // Parse the outer S3 error record
          const s3Record: S3ErrorRecord = JSON.parse(line);
          
          // Decode the base64 rawData
          const decodedData = Buffer.from(s3Record.rawData, 'base64').toString('utf-8');
            
          
          // Parse the decoded data as JSON
          const firehoseRecord: FirehoseRecord = JSON.parse(decodedData);
          records.push(firehoseRecord);
        } catch (parseError) {
          console.warn(`Failed to parse/decode line from ${key}:`, line.substring(0, 100));
        }
      }

      return records;
    } catch (error) {
      console.error(`Error downloading/decoding ${key}:`, error);
      return [];
    }
  }


  /**
   * Send records to Firehose in batches and return success status
   */
  private async sendToFirehose(records: FirehoseRecord[]): Promise<boolean> {
    if (records.length === 0) {
      console.log('No records to send to Firehose');
      return true;
    }

    // Extract delivery stream name from ARN
    // ARN format: arn:aws:firehose:region:account-id:deliverystream/name
    const deliveryStreamName = this.firehoseArn.split('/').pop();
    if (!deliveryStreamName) {
      throw new Error('Invalid Firehose ARN format');
    }

    console.log(`\nSending ${records.length} records to Firehose: ${deliveryStreamName}`);

    // Firehose allows up to 500 records per batch
    const batchSize = 500;
    let successCount = 0;
    let failureCount = 0;

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      
      const firehoseRecords = batch.map(record => ({
        Data: Buffer.from(JSON.stringify(record) + '\n'),
      }));

      try {
        const command = new PutRecordBatchCommand({
          DeliveryStreamName: deliveryStreamName,
          Records: firehoseRecords,
        });

        const response = await this.firehoseClient.send(command);
        
        successCount += (response.RequestResponses?.length || 0) - (response.FailedPutCount || 0);
        failureCount += response.FailedPutCount || 0;

        console.log(`Batch ${Math.floor(i / batchSize) + 1}: ${batch.length} records sent (${response.FailedPutCount || 0} failures)`);
      } catch (error) {
        console.error(`Error sending batch ${Math.floor(i / batchSize) + 1}:`, error);
        failureCount += batch.length;
      }
    }

    console.log(`\nTotal: ${successCount} successful, ${failureCount} failed`);
    return failureCount === 0;
  }

  /**
   * Delete S3 objects after successful processing (parallel with concurrency)
   */
  private async deleteS3Objects(keys: string[]): Promise<void> {
    if (keys.length === 0) {
      return;
    }

    console.log(`\nDeleting ${keys.length} processed S3 objects with concurrency ${this.concurrency}...`);

    const results = await Bluebird.map(
      keys,
      async (key: string) => {
        try {
          const command = new DeleteObjectCommand({
            Bucket: this.s3Bucket,
            Key: key,
          });

          await this.s3Client.send(command);
          console.log(`  ✓ Deleted: ${key}`);
          return { key, success: true };
        } catch (error) {
          console.error(`  ✗ Failed to delete ${key}:`, error);
          return { key, success: false, error };
        }
      },
      { concurrency: this.concurrency }
    );

    const successCount = results.filter(r => r.success).length;
    const failureCount = results.filter(r => !r.success).length;

    console.log(`\nDeletion complete: ${successCount} successful, ${failureCount} failed`);
  }

  /**
   * Main execution method
   */
  async run(): Promise<void> {
    try {
      console.log('Starting Firehose error retry process...\n');

      // Step 1: List all S3 objects from the last 5 days
      const s3Keys = await this.listS3Objects();

      if (s3Keys.length === 0) {
        console.log('No objects found in the specified time range');
        return;
      }

      // Step 2: Download, decode, and parse all objects in parallel
      console.log(`\nDownloading and parsing ${s3Keys.length} S3 objects with concurrency ${this.concurrency}...\n`);
      const processedFiles: ProcessedFile[] = [];
      const eventNames = new Set<string>();
      
      // Process S3 keys in parallel with concurrency limit
      const results = await Bluebird.map(
        s3Keys,
        async (key: string) => {
          console.log(`  Processing: ${key}`);
          const records = await this.downloadAndDecodeS3Object(key);

          // Track event names
          records.forEach(record => {
            if (record.eventName) {
              eventNames.add(record.eventName);
            }
          });

          if (records.length > 0) {
            console.log(`    ✓ Found ${records.length} event(s)`);
            return { key, records, totalRecords: records.length };
          } else {
            console.log(`    - No events`);
            return { key, records: [], totalRecords: 0 };
          }
        },
        { concurrency: this.concurrency }
      );

      // Collect results
      let totalRecordsProcessed = 0;

      results.forEach(result => {
        totalRecordsProcessed += result.totalRecords;
        if (result.records.length > 0) {
          processedFiles.push({ key: result.key, records: result.records });
        }
      });

      console.log(`\nTotal records to process: ${totalRecordsProcessed}`);
      console.log(`Event names found: ${Array.from(eventNames).join(', ')}`)

      // Step 3: Process files and send to Firehose in parallel
      console.log(`\n📤 Sending ${processedFiles.length} file(s) to Firehose with concurrency ${this.concurrency}...\n`);
      
      const sendResults = await Bluebird.map(
        processedFiles,
        async (file: ProcessedFile) => {
          console.log(`  Processing: ${file.key} (${file.records.length} record(s))`);
          
          // Send to Firehose
          const success = await this.sendToFirehose(file.records);

          if (success) {
            console.log(`    ✓ Successfully sent ${file.records.length} record(s)`);
            return { key: file.key, success: true, recordCount: file.records.length };
          } else {
            console.log(`    ✗ Failed to send records. Skipping deletion.`);
            return { key: file.key, success: false, recordCount: file.records.length };
          }
        },
        { concurrency: this.concurrency }
      );

      // Collect successful files for deletion
      const filesToDelete = sendResults
        .filter(result => result.success)
        .map(result => result.key);

      const sentCount = sendResults
        .filter(result => result.success)
        .reduce((sum, result) => sum + result.recordCount, 0);

      // Step 4: Delete successfully processed S3 files
      if (filesToDelete.length > 0) {
        await this.deleteS3Objects(filesToDelete);
      }

      const failedCount = sendResults.filter(result => !result.success).length;
      console.log(`\n📊 Summary: Sent ${sentCount} records from ${sendResults.length} file(s), deleted ${filesToDelete.length} S3 file(s)${failedCount > 0 ? `, ${failedCount} failed` : ''}`);

      console.log('\n✓ Process completed successfully!');
    } catch (error) {
      console.error('\n✗ Error during execution:', error);
      throw error;
    }
  }
}

// Main execution
(async () => {
  try {
    const retryTool = new FirehoseErrorRetry();
    await retryTool.run();
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
})();
