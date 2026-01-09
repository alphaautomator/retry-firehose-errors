import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { FirehoseClient, PutRecordBatchCommand } from '@aws-sdk/client-firehose';
import * as dotenv from 'dotenv';
import { Readable } from 'stream';

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

class FirehoseErrorRetry {
  private s3Client: S3Client;
  private firehoseClient: FirehoseClient;
  private s3Bucket: string;
  private s3Prefix: string;
  private firehoseArn: string;
  private readonly maxEventsToProcess: number = 10;

  constructor() {
    const region = process.env.AWS_REGION || 'us-east-1';
    
    this.s3Client = new S3Client({ region });
    this.firehoseClient = new FirehoseClient({ region });
    
    this.s3Bucket = process.env.S3_BUCKET || '';
    this.s3Prefix = process.env.S3_PREFIX || '';
    this.firehoseArn = process.env.FIREHOSE_ARN || '';

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
  }

  /**
   * Get the date 5 days ago
   */
  private getFiveDaysAgo(): Date {
    const date = new Date();
    date.setDate(date.getDate() - 1);
    date.setHours(0, 0, 0, 0);
    return date;
  }

  /**
   * List all S3 objects that are 5 days old
   */
  private async listS3Objects(): Promise<string[]> {
    const fiveDaysAgo = this.getFiveDaysAgo();
    console.log(`\nFetching objects from ${fiveDaysAgo.toISOString()} onwards...`);
    
    const objects: string[] = [];
    let continuationToken: string | undefined;

    do {
      const command = new ListObjectsV2Command({
        Bucket: this.s3Bucket,
        Prefix: this.s3Prefix,
        ContinuationToken: continuationToken,
      });

      const response = await this.s3Client.send(command);

      if (response.Contents) {
        for (const obj of response.Contents) {
          if (obj.Key && obj.LastModified) {
            // Check if the object is from 5 days ago
            if (obj.LastModified >= fiveDaysAgo) {
              objects.push(obj.Key);
            }
          }
        }
      }

      continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    console.log(`Found ${objects.length} objects from the last 5 days`);
    return objects;
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
   * Filter records by eventName
   */
  private filterAppointmentPatchedRecords(records: FirehoseRecord[]): FirehoseRecord[] {
    return records.filter(record => record.eventName === 'appointmentPatched');
  }

  /**
   * Send records to Firehose in batches
   */
  private async sendToFirehose(records: FirehoseRecord[]): Promise<void> {
    if (records.length === 0) {
      console.log('No records to send to Firehose');
      return;
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

      // Step 2: Download, decode, and parse objects until we have enough records
      console.log('\nDownloading and parsing S3 objects...');
      let filteredRecords: FirehoseRecord[] = [];
      let totalRecordsProcessed = 0;
      const eventNames = new Set<string>();
      
      for (const key of s3Keys) {
        if (filteredRecords.length >= this.maxEventsToProcess) {
          console.log(`\n✓ Reached target of ${this.maxEventsToProcess} records. Stopping S3 fetch.`);
          break;
        }

        console.log(`Processing: ${key}`);
        const records = await this.downloadAndDecodeS3Object(key);
        totalRecordsProcessed += records.length;

        // Track event names
        records.forEach(record => {
          if (record.eventName) {
            eventNames.add(record.eventName);
          }
        });

        // Filter and add appointmentPatched events
        const matchingRecords = this.filterAppointmentPatchedRecords(records);
        filteredRecords = filteredRecords.concat(matchingRecords);

        console.log(`  Found ${matchingRecords.length} appointmentPatched events (total so far: ${filteredRecords.length})`);
      }

      console.log(`\nTotal records processed: ${totalRecordsProcessed}`);
      console.log(`Event names found: ${Array.from(eventNames).join(', ')}`);
      console.log(`Records with eventName='appointmentPatched': ${filteredRecords.length}`);
      console.log('Filtered records:', filteredRecords.map(record => record.eventData?.id));
      // Step 3: Limit to max events (in case last file pushed us over)
      const recordsToSend = filteredRecords.slice(0, this.maxEventsToProcess);
      if (filteredRecords.length > this.maxEventsToProcess) {
        console.log(`⚠️  Limiting to ${this.maxEventsToProcess} events (found ${filteredRecords.length} total)`);
      }

      // Step 4: Send to Firehose
      await this.sendToFirehose(recordsToSend);

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
