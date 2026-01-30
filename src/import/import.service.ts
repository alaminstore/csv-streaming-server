import { Injectable, BadRequestException } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { PrismaService } from '../prisma/prisma.service';
import { Subject } from 'rxjs';

export interface ProgressEvent {
  jobId: string;
  status: string;
  processedRows: number;
  totalRows: number;
  failedRows: number;
  progress: number;
  startedAt: Date | null;
  estimatedCompletion: Date | null;
  totalPages: number;
  recentRows: any[];
}

@Injectable()
export class ImportService {
  // Subject for SSE progress updates
  public progressSubject = new Subject<ProgressEvent>();

  constructor(
    @InjectQueue('csv-import') private importQueue: Queue,
    private prisma: PrismaService,
  ) {}

  async startImport() {
    // Check if there's already a running import
    const runningJob = await this.prisma.importJob.findFirst({
      where: {
        status: { in: ['pending', 'processing'] },
      },
    });

    if (runningJob) {
      throw new BadRequestException(
        'An import is already running, please refresh the page.',
      );
    }

    // Create new import job record
    const importJob = await this.prisma.importJob.create({
      data: {
        status: 'pending',
        totalRows: 2000000,
        processedRows: 0,
        failedRows: 0,
        progress: 0,
      },
    });

    // Add job to BullMQ queue
    await this.importQueue.add('process-csv', {
      jobId: importJob.id,
    });

    return importJob;
  }

  async getLatestJob() {
    return this.prisma.importJob.findFirst({
      orderBy: { createdAt: 'desc' },
    });
  }

  // Emit progress update for SSE
  emitProgress(progress: ProgressEvent) {
    this.progressSubject.next(progress);
  }
}
