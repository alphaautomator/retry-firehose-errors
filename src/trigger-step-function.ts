import { SFNClient, StartExecutionCommand, DescribeExecutionCommand, RedriveExecutionCommand } from '@aws-sdk/client-sfn';
import * as dotenv from 'dotenv';
import Bluebird from 'bluebird';

// Load environment variables
dotenv.config();

interface ExecutionResult {
  time: string;
  executionArn?: string;
  success: boolean;
  skipped?: boolean;
  redriven?: boolean;
  error?: string;
}

class StepFunctionTrigger {
  private sfnClient: SFNClient;
  private stateMachineArn: string;
  private startDate: Date;
  private endDate: Date;
  private intervalMinutes: number = 5;
  private readonly concurrency: number = 1;

  constructor() {
    const region = process.env.AWS_REGION || 'us-east-1';
    this.sfnClient = new SFNClient({ region });
    
    this.stateMachineArn = process.env.STATE_MACHINE_ARN || '';
    
    if (!this.stateMachineArn) {
      throw new Error('STATE_MACHINE_ARN is not defined in .env file');
    }

    // Parse date range (ensure UTC)
    const startDateStr = process.env.START_DATE || '2025-12-25';
    const endDateStr = process.env.END_DATE || '2026-01-11';
    
    // Parse dates as UTC midnight
    this.startDate = new Date(`${startDateStr}T00:00:00.000Z`);
    this.endDate = new Date(`${endDateStr}T23:59:59.999Z`);

    const totalDays = Math.ceil((this.endDate.getTime() - this.startDate.getTime()) / (1000 * 60 * 60 * 24)) + 1;
    const totalExecutions = totalDays * ((24 * 60) / this.intervalMinutes);

    console.log(`Initialized with:`);
    console.log(`  State Machine ARN: ${this.stateMachineArn}`);
    console.log(`  Date Range: ${this.startDate.toISOString().split('T')[0]} to ${this.endDate.toISOString().split('T')[0]}`);
    console.log(`  Days: ${totalDays}`);
    console.log(`  Interval: ${this.intervalMinutes} minutes`);
    console.log(`  Total executions: ${totalExecutions} (${(24 * 60) / this.intervalMinutes} per day)`);
    console.log(`  Concurrency: ${this.concurrency}`);
  }

  /**
   * Generate all dates between start and end date (UTC)
   */
  private generateDateRange(): Date[] {
    const dates: Date[] = [];
    const currentDate = new Date(this.startDate);

    while (currentDate <= this.endDate) {
      dates.push(new Date(currentDate));
      // Increment by 1 day in UTC
      currentDate.setUTCDate(currentDate.getUTCDate() + 1);
    }

    return dates;
  }

  /**
   * Generate all time slots for a specific day in 5-minute intervals (UTC)
   */
  private generateTimeSlotsForDay(date: Date): Date[] {
    const slots: Date[] = [];
    
    // Get year, month, day from the date (in UTC)
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth();
    const day = date.getUTCDate();

    // Generate slots from 00:00 to 23:55 (last slot of the day) in UTC
    for (let minutes = 0; minutes < 24 * 60; minutes += this.intervalMinutes) {
      const hours = Math.floor(minutes / 60);
      const mins = minutes % 60;
      
      // Create date in UTC
      const slot = new Date(Date.UTC(year, month, day, hours, mins, 0, 0));
      slots.push(slot);
    }

    return slots;
  }

  /**
   * Generate all time slots for the date range
   */
  private generateTimeSlots(): Date[] {
    const allSlots: Date[] = [];
    const dates = this.generateDateRange();

    for (const date of dates) {
      const daySlots = this.generateTimeSlotsForDay(date);
      allSlots.push(...daySlots);
    }

    return allSlots;
  }

  /**
   * Format date to ISO string for payload
   */
  private formatTimeForPayload(date: Date): string {
    return date.toISOString();
  }

  /**
   * Generate a unique execution name
   */
  private generateExecutionName(time: Date): string {
    const timestamp = time.toISOString().replace(/[:.]/g, '-').replace('Z', '');
    return `retry-execution-1-${timestamp}`;
  }

  /**
   * Check if execution exists and get its status
   */
  private async checkExecutionStatus(executionName: string): Promise<{ exists: boolean; status?: string; arn?: string }> {
    try {
      // Construct the execution ARN
      const executionArn = `${this.stateMachineArn.replace(':stateMachine:', ':execution:')}:${executionName}`;
      
      const command = new DescribeExecutionCommand({
        executionArn,
      });

      const response = await this.sfnClient.send(command);
      
      return {
        exists: true,
        status: response.status,
        arn: executionArn,
      };
    } catch (error: any) {
      // If execution doesn't exist, DescribeExecution will throw an error
      if (error.name === 'ExecutionDoesNotExist' || error.name === 'ResourceNotFound') {
        return { exists: false };
      }
      throw error;
    }
  }

  /**
   * Trigger Step Function with payload (with smart retry logic using redrive)
   */
  private async triggerStepFunction(time: Date): Promise<ExecutionResult> {
    const timeStr = this.formatTimeForPayload(time);
    const payload = {
      time: timeStr,
    };

    const executionName = this.generateExecutionName(time);
    
    // // Check if execution already exists
    // const existingExecution = await this.checkExecutionStatus(executionName);
    
    // if (existingExecution.exists) {
    //   const status = existingExecution.status;
      
    //   // If execution exists and succeeded or is running, skip it
    //   if (status === 'SUCCEEDED' || status === 'RUNNING') {
    //     return {
    //       time: timeStr,
    //       executionArn: existingExecution.arn,
    //       success: true,
    //       skipped: true,
    //     };
    //   }
      
    //   // If execution failed, use redrive to retry it
    //   if (status === 'FAILED' || status === 'TIMED_OUT' || status === 'ABORTED') {
    //     try {
    //       const redriveCommand = new RedriveExecutionCommand({
    //         executionArn: existingExecution.arn,
    //       });

    //       const redriveResponse = await this.sfnClient.send(redriveCommand);

    //       return {
    //         time: timeStr,
    //         executionArn: redriveResponse.redriveDate ? existingExecution.arn : undefined,
    //         success: true,
    //         redriven: true,
    //       };
    //     } catch (error: any) {
    //       return {
    //         time: timeStr,
    //         success: false,
    //         error: `Redrive failed: ${error.message || String(error)}`,
    //       };
    //     }
    //   }
    // }

    // Execution doesn't exist, create it
    try {
      const command = new StartExecutionCommand({
        stateMachineArn: this.stateMachineArn,
        input: JSON.stringify(payload),
        name: executionName,
      });

      const response = await this.sfnClient.send(command);

      return {
        time: timeStr,
        executionArn: response.executionArn,
        success: true,
      };
    } catch (error: any) {
      return {
        time: timeStr,
        success: false,
        error: error.message || String(error),
      };
    }
  }

  /**
   * Main execution method with parallel execution
   */
  async run(): Promise<void> {
    console.log('\n🚀 Starting Step Function trigger process...\n');

    const timeSlots = this.generateTimeSlots();
    console.log(`Triggering ${timeSlots.length} executions with concurrency ${this.concurrency}...\n`);

    // Trigger executions in parallel with concurrency limit
    const results = await Bluebird.map(
      timeSlots,
      async (slot: Date, index: number) => {
        const timeStr = this.formatTimeForPayload(slot);
        console.log(`  [${index + 1}/${timeSlots.length}] Triggering: ${timeStr}`);
        
        const result = await this.triggerStepFunction(slot);
        
        if (result.success) {
          if (result.skipped) {
            console.log(`    ⊙ Skipped (already succeeded)`);
          } else if (result.redriven) {
            console.log(`    🔄 Redriven (retrying failed execution)`);
          } else {
            console.log(`    ✓ Success`);
          }
        } else {
          console.log(`    ✗ Failed: ${result.error}`);
        }
        
        return result;
      },
      { concurrency: this.concurrency }
    );

    // Calculate statistics
    const newExecutions = results.filter(r => r.success && !r.skipped && !r.redriven).length;
    const redrivenCount = results.filter(r => r.redriven).length;
    const skippedCount = results.filter(r => r.skipped).length;
    const failureCount = results.filter(r => !r.success).length;

    // Summary
    console.log('\n' + '='.repeat(70));
    console.log('📊 Summary');
    console.log('='.repeat(70));
    console.log(`Total executions: ${timeSlots.length}`);
    console.log(`New executions: ${newExecutions}`);
    console.log(`Redriven (retried failed): ${redrivenCount}`);
    console.log(`Skipped (already succeeded): ${skippedCount}`);
    console.log(`Failed: ${failureCount}`);
    console.log('='.repeat(70));

    // Show failures if any
    if (failureCount > 0) {
      console.log('\n❌ Failed executions:');
      results
        .filter(r => !r.success)
        .forEach(r => {
          console.log(`  ${r.time}: ${r.error}`);
        });
    }

    console.log('\n✓ Process completed!');
  }
}

// Main execution
(async () => {
  try {
    const trigger = new StepFunctionTrigger();
    await trigger.run();
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
})();
