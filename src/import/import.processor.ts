import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { ImportService } from './import.service';
import * as fs from 'fs';
import csvParser from 'csv-parser';
import { CsvCustomerRowDto } from './dto/csv-customer-row.dto';
import { ImportRecentCustomerDto } from './dto/import-recent-customer.dto';

@Processor('csv-import')
@Injectable()
export class ImportProcessor extends WorkerHost {
  private readonly logger = new Logger(ImportProcessor.name);

  private readonly batchSize: number = parseInt(
    process.env.CSV_BATCH_SIZE || '1000',
    10,
  );

  constructor(
    private prisma: PrismaService,
    private importService: ImportService,
  ) {
    super();
  }

  async process(job: Job<{ jobId: string }>): Promise<void> {
    const { jobId } = job.data;
    const csvFilePath = this.getCsvPath();

    this.logger.log(`Starting CSV import for job ${jobId}`);

    await this.markJobAsProcessing(jobId);

    // total rows calculation is not mendatory as per current design,
    // but we added it for future use or we can comment it out in the frontend if we needed to see total rows in the UI.
    const totalRows = await this.countTotalCsvRows(csvFilePath);
    await this.updateTotalRows(jobId, totalRows);

    const jobState = await this.getJobProgressState(jobId);

    const result = await this.startCsvStreaming({
      jobId,
      csvFilePath,
      totalRows,
      ...jobState,
    });

    await this.completeJob(
      jobId,
      result.processedRows,
      result.failedRows,
      result.recentRows,
      totalRows,
    );
  }

  private getCsvPath(): string {
    return process.env.CSV_FILE_PATH || './data/customers.csv';
  }

  private async markJobAsProcessing(jobId: string) {
    await this.prisma.importJob.update({
      where: { id: jobId },
      data: { status: 'processing', startedAt: new Date() },
    });
  }

  private async updateTotalRows(jobId: string, totalRows: number) {
    await this.prisma.importJob.update({
      where: { id: jobId },
      data: { totalRows },
    });
  }

  private async getJobProgressState(jobId: string) {
    const importJob = await this.prisma.importJob.findUnique({
      where: { id: jobId },
    });

    return {
      processedRows: importJob?.processedRows || 0,
      failedRows: importJob?.failedRows || 0,
      startedAt: importJob?.startedAt || new Date(),
    };
  }

  private async countTotalCsvRows(filePath: string): Promise<number> {
    return new Promise((resolve) => {
      let lines = 0;
      fs.createReadStream(filePath)
        .on('data', (chunk) => {
          for (const byte of chunk) {
            if (byte === 10) lines++;
          }
        })
        .on('end', () => resolve(lines));
    });
  }

  private async startCsvStreaming(params: {
    jobId: string;
    csvFilePath: string;
    totalRows: number;
    processedRows: number;
    failedRows: number;
    startedAt: Date;
  }) {
    const { jobId, csvFilePath, totalRows, startedAt } = params;

    let { processedRows, failedRows } = params;
    let batch: any[] = [];
    let recentRows: any[] = [];
    let rowIndex = 0;
    const startTime = Date.now();

    const stream = fs
      .createReadStream(csvFilePath)
      .pipe(csvParser({ mapHeaders: ({ header }) => header.trim() }));

    for await (const row of stream as AsyncIterable<any>) {
      rowIndex++;

      // skip already processed rows
      // It can happen if the process restarts.
      if (rowIndex <= processedRows) continue;

      // I know cancelled job option not available in the UI yet.
      // but we added it for future use.
      // If we just update the job status to 'cancelled' from UI or DB directly,
      // this will stop the process.
      if (await this.isJobCancelled(jobId, rowIndex)) {
        this.logger.log(`Job ${jobId} was cancelled`);
        stream.destroy();
        break;
      }

      batch.push(this.customerRowMapping(row));

      if (batch.length >= this.batchSize) {
        stream.pause();

        const batchResult = await this.processBatch(batch);

        processedRows += batchResult.success;
        failedRows += batchResult.failed;
        recentRows = this.updateRecentRows(batch, recentRows);

        await this.updateProgress({
          jobId,
          processedRows,
          failedRows,
          totalRows,
          recentRows,
          startedAt,
          startTime,
        });

        batch = [];
        stream.resume();
      }
    }

    // handle remaining rows (if last chunk is less than the batch size)
    const finalResult = await this.processRemainingBatch(
      batch,
      processedRows,
      failedRows,
      recentRows,
    );

    return finalResult;
  }

  private async isJobCancelled(
    jobId: string,
    rowIndex: number,
  ): Promise<boolean> {
    if (rowIndex % 1000 !== 0) return false;

    const job = await this.prisma.importJob.findUnique({
      where: { id: jobId },
    });
    return job?.status === 'cancelled';
  }

  private customerRowMapping(row: CsvCustomerRowDto) {
    return {
      customerId: row['Customer Id'],
      firstName: row['First Name'],
      lastName: row['Last Name'],
      company: row.Company,
      city: row.City,
      country: row.Country,
      phone1: row['Phone 1'],
      phone2: row['Phone 2'],
      email: row.Email,
      subscriptionDate: new Date(row['Subscription Date']),
      website: row.Website,
      about: row['About Customer'] || '',
    };
  }

  private async processBatch(batch: any[]) {
    try {
      const results = await Promise.allSettled(
        batch.map((customer) =>
          this.prisma.customer.upsert({
            where: { customerId: customer.customerId },
            create: customer,
            update: customer,
          }),
        ),
      );

      return {
        success: results.filter((r) => r.status === 'fulfilled').length,
        failed: results.filter((r) => r.status === 'rejected').length,
      };
    } catch (error) {
      this.logger.error(`Error inserting batch: ${error.message}`);
      return { success: 0, failed: batch.length };
    }
  }

  private async processRemainingBatch(
    batch: any[],
    processedRows: number,
    failedRows: number,
    recentRows: any[],
  ) {
    if (!batch.length) {
      return { processedRows, failedRows, recentRows };
    }

    try {
      const results = await Promise.allSettled(
        batch.map((customer) =>
          this.prisma.customer.upsert({
            where: { customerId: customer.customerId },
            create: customer,
            update: customer,
          }),
        ),
      );

      const successCount = results.filter(
        (r) => r.status === 'fulfilled',
      ).length;
      const failCount = results.filter((r) => r.status === 'rejected').length;

      processedRows += successCount;
      failedRows += failCount;

      // Keep recent rows for UI display (important for small CSVs)
      recentRows = this.updateRecentRows(batch, recentRows);
    } catch (error) {
      this.logger.error(`Error inserting final batch: ${error.message}`);
      failedRows += batch.length;
    }

    return { processedRows, failedRows, recentRows };
  }

  private updateRecentRows(
    batch: CsvCustomerRowDto[],
    recentRows: CsvCustomerRowDto[],
  ): CsvCustomerRowDto[] {
    return [...batch.slice(-50), ...recentRows].slice(0, 50);
  }

  private async updateProgress(data: {
    jobId: string;
    processedRows: number;
    failedRows: number;
    totalRows: number;
    recentRows: any[];
    startedAt: Date;
    startTime: number;
  }) {
    const {
      jobId,
      processedRows,
      failedRows,
      totalRows,
      recentRows,
      startedAt,
      startTime,
    } = data;

    const progress = (processedRows / totalRows) * 100;
    const elapsed = Date.now() - startTime;
    const rowsPerMs = processedRows / elapsed;
    const remainingRows = totalRows - processedRows;
    const eta = new Date(Date.now() + remainingRows / rowsPerMs);

    await this.prisma.importJob.update({
      where: { id: jobId },
      data: {
        processedRows,
        failedRows,
        progress,
        estimatedCompletion: eta,
        recentRows: recentRows.map((row) => ({
          customerId: row.customerId,
          firstName: row.firstName,
          lastName: row.lastName,
          email: row.email,
          company: row.company,
        })),
        updatedAt: new Date(),
      },
    });

    this.importService.emitProgress({
      jobId,
      status: 'processing',
      processedRows,
      totalRows,
      failedRows,
      progress,
      startedAt,
      estimatedCompletion: eta,
      totalPages: await this.getTotalPages(),
      recentRows: recentRows.slice(0, 10),
    });

    this.logger.log(
      `Processed ${processedRows}/${totalRows} rows (${progress.toFixed(2)}%)`,
    );
  }

  private async completeJob(
    jobId: string,
    processedRows: number,
    failedRows: number,
    recentRows: any[],
    totalRows: number,
  ) {
    await this.prisma.importJob.update({
      where: { id: jobId },
      data: {
        status: 'completed',
        processedRows,
        failedRows,
        progress: 100,
        completedAt: new Date(),
      },
    });

    this.importService.emitProgress({
      jobId,
      status: 'completed',
      processedRows,
      totalRows,
      failedRows,
      progress: 100,
      startedAt:
        (await this.prisma.importJob.findUnique({ where: { id: jobId } }))
          ?.startedAt || new Date(),
      estimatedCompletion: null,
      recentRows: recentRows.slice(0, 10).map((row) => ({
        customerId: row.customerId,
        firstName: row.firstName,
        lastName: row.lastName,
        email: row.email,
        company: row.company,
      })),
      totalPages: await this.getTotalPages(),
    });

    this.logger.log(
      `CSV import completed. Processed: ${processedRows}, Failed: ${failedRows}`,
    );
  }

  private async getTotalPages(limit: number = 50) {
    const total = await this.prisma.customer.count();
    return Math.ceil(total / limit);
  }
}
