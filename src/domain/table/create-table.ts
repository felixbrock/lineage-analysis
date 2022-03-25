import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Table } from '../entities/table';
// todo cleancode violation
import { ObjectId } from 'mongodb';
import { ReadTables } from './read-tables';
import { ITableRepo } from './i-table-repo';

export interface CreateTableRequestDto {
  name: string;
  modelId: string;
  lineageId: string;
}

export interface CreateTableAuthDto {
  organizationId: string;
}

export type CreateTableResponseDto = Result<Table>;

export class CreateTable
  implements
    IUseCase<CreateTableRequestDto, CreateTableResponseDto, CreateTableAuthDto>
{
  #readTables: ReadTables;

  #tableRepo: ITableRepo;

  constructor(readTables: ReadTables, tableRepo: ITableRepo) {
    this.#readTables = readTables;
    this.#tableRepo = tableRepo;
  }

  async execute(
    request: CreateTableRequestDto,
    auth: CreateTableAuthDto
  ): Promise<CreateTableResponseDto> {
    try {
      const table = Table.create({
        id: new ObjectId().toHexString(),
        name: request.name,
        modelId: request.modelId,
        lineageId: request.lineageId,
      });

      const readTablesResult = await this.#readTables.execute(
        {
          modelId: request.modelId,
          name: request.name,
        },
        { organizationId: auth.organizationId }
      );

      if (!readTablesResult.success) throw new Error(readTablesResult.error);
      if (!readTablesResult.value) throw new Error('Reading tables failed');
      if (readTablesResult.value.length)
        throw new Error(`Table already exists`);

      await this.#tableRepo.insertOne(table);

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
