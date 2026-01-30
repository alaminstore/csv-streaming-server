import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ImportService } from './import.service';
import { ImportProcessor } from './import.processor';

@Module({
  imports: [
    BullModule.registerQueue({
      name: 'csv-import',
    }),
  ],
  providers: [ImportService, ImportProcessor],
  exports: [ImportService],
})
export class ImportModule {}
