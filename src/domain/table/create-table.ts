import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { buildTableDto, TableDto } from './table-dto';
import { Table } from '../entities/table';
// todo cleancode violation
import { ObjectId } from 'mongodb';

export interface CreateTableRequestDto {
  name: string;
  modelId: string;
}

export interface CreateTableAuthDto {
  organizationId: string;
}

export type CreateTableResponseDto = Result<Table>;

export class CreateTable
  implements
    IUseCase<CreateTableRequestDto, CreateTableResponseDto, CreateTableAuthDto>
{
  constructor() {}

  async execute(
    request: CreateTableRequestDto,
    auth: CreateTableAuthDto
  ): Promise<CreateTableResponseDto> {
    try {
      const table = Table.create({
        id: new ObjectId().toHexString(),
        name: request.name,
        modelId: request.modelId,
      });

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(table);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
