import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column } from '../entities/column';
import { ReadColumns } from './read-columns';
import { IColumnRepo } from './i-column-repo';

export interface CreateColumnRequestDto {
  name: string;
  dbtModelId: string;
  tableId: string;
  lineageId: string;
}

export interface CreateColumnAuthDto {
  organizationId: string;
}

export type CreateColumnResponseDto = Result<Column>;

export class CreateColumn
  implements
    IUseCase<
      CreateColumnRequestDto,
      CreateColumnResponseDto,
      CreateColumnAuthDto
    >
{
  readonly #columnRepo: IColumnRepo;

  readonly #readColumns: ReadColumns;

  constructor(readColumns: ReadColumns, columnRepo: IColumnRepo) {
    this.#readColumns = readColumns;
    this.#columnRepo = columnRepo;
  }

  async execute(
    request: CreateColumnRequestDto,
    auth: CreateColumnAuthDto
  ): Promise<CreateColumnResponseDto> {
    try {
      const column = Column.create({
        id: new ObjectId().toHexString(),
        dbtModelId: request.dbtModelId,
        name: request.name,
        tableId: request.tableId,
        lineageId: request.lineageId,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: request.name,
          tableId: request.tableId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for table already exists`);

      await this.#columnRepo.insertOne(column);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(column);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
