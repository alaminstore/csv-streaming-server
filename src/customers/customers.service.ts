import {
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { CreateCustomerDto } from './dto/create-customer.dto';
import { UpdateCustomerDto } from './dto/update-customer.dto';
import { ImportService } from '../import/import.service';
import { Observable, map } from 'rxjs';

interface MessageEvent {
  data: string | object;
}

@Injectable()
export class CustomersService {
  constructor(
    private prisma: PrismaService,
    private importService: ImportService,
  ) {}

  // Sync methods - delegate to ImportService
  async startSync() {
    return this.importService.startImport();
  }

  async getSyncStatus() {
    return this.importService.getLatestJob();
  }

  getSyncProgressStream(): Observable<MessageEvent> {
    return this.importService.progressSubject.pipe(
      map((event) => ({
        data: event,
      })),
    );
  }

  async create(createCustomerDto: CreateCustomerDto) {
    try {
      return await this.prisma.customer.create({
        data: createCustomerDto,
      });
    } catch (error) {
      if (error?.code === 'P2002') {
        throw new ConflictException(
          'Customer already exists with this Customer ID',
        );
      }
      throw error;
    }
  }

  async findAll(page: number = 1, limit: number = 50) {
    const skip = (page - 1) * limit;

    const [customers, total] = await Promise.all([
      this.prisma.customer.findMany({
        skip,
        take: limit,
        orderBy: { createdAt: 'desc' },
      }),
      this.prisma.customer.count(),
    ]);

    return {
      data: customers,
      meta: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  async update(id: string, updateCustomerDto: UpdateCustomerDto) {
    try {
      return await this.prisma.customer.update({
        where: { id },
        data: updateCustomerDto,
      });
    } catch (error) {
      throw new NotFoundException(`Customer with ID ${id} not found`);
    }
  }

  async remove(id: string) {
    try {
      return await this.prisma.customer.delete({
        where: { id },
      });
    } catch (error) {
      throw new NotFoundException(`Customer with ID ${id} not found`);
    }
  }
}
