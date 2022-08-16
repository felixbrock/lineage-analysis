import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column } from '../entities/column';
import { ReadColumns } from './read-columns';
import { IColumnRepo } from './i-column-repo';
import { DbConnection } from '../services/i-db';

export interface CreateColumnRequestDto {
  dbtModelId: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId: string;
}

export interface CreateColumnAuthDto {
  isSystemInternal: boolean;
}

export type CreateColumnResponseDto = Result<Column>;

export class CreateColumn
  implements
    IUseCase<
      CreateColumnRequestDto,
      CreateColumnResponseDto,
      CreateColumnAuthDto,
      DbConnection
    >
{
  readonly #columnRepo: IColumnRepo;

  readonly #readColumns: ReadColumns;

  #dbConnection: DbConnection;

  constructor(readColumns: ReadColumns, columnRepo: IColumnRepo) {
    this.#readColumns = readColumns;
    this.#columnRepo = columnRepo;
  }

  async execute(
    request: CreateColumnRequestDto,
    auth: CreateColumnAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateColumnResponseDto> {
    try {
      if (!auth.isSystemInternal) throw new Error('Unauthorized');

      this.#dbConnection = dbConnection;

      const column = Column.create({
        id: new ObjectId().toHexString(),
        dbtModelId: request.dbtModelId,
        name: request.name,
        index: request.index,
        type: request.type,
        materializationId: request.materializationId,
        lineageId: request.lineageId,
        organizationId: request.targetOrganizationId,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: request.name,
          materializationId: request.materializationId,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId,
        },
        {  isSystemInternal: auth.isSystemInternal },
        this.#dbConnection
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for materialization already exists`);

      if (request.writeToPersistence)
        await this.#columnRepo.insertOne(column, this.#dbConnection);

      return Result.ok(column);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
