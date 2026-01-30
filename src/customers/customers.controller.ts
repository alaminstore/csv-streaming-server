import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  Query,
  Sse,
} from '@nestjs/common';
import { CustomersService } from './customers.service';
import { CreateCustomerDto } from './dto/create-customer.dto';
import { UpdateCustomerDto } from './dto/update-customer.dto';
import { Observable } from 'rxjs';

interface MessageEvent {
  data: string | object;
}

@Controller('customers')
export class CustomersController {
  constructor(private readonly customersService: CustomersService) {}

  // Sync endpoint - starts CSV import
  @Post('sync')
  async startSync() {
    return await this.customersService.startSync();
  }

  // Get sync status
  @Get('sync/status')
  async getSyncStatus() {
    return await this.customersService.getSyncStatus();
  }

  // SSE endpoint for real-time sync progress
  @Sse('sync/stream')
  streamSyncProgress(): Observable<MessageEvent> {
    return this.customersService.getSyncProgressStream();
  }

  @Post()
  create(@Body() createCustomerDto: CreateCustomerDto) {
    return this.customersService.create(createCustomerDto);
  }

  @Get()
  findAll(@Query('page') page?: string, @Query('limit') limit?: string) {
    return this.customersService.findAll(
      page ? parseInt(page, 10) : 1,
      limit ? parseInt(limit, 10) : 50,
    );
  }

  @Patch(':id')
  update(
    @Param('id') id: string,
    @Body() updateCustomerDto: UpdateCustomerDto,
  ) {
    return this.customersService.update(id, updateCustomerDto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.customersService.remove(id);
  }
}
