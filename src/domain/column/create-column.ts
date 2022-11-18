import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column, ColumnDataType } from '../entities/column';
import { ReadColumns } from './read-columns';
import { IColumnRepo } from './i-column-repo';
import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

export interface CreateColumnRequestDto {
  relationName: string;
  name: string;
  index: string;
  dataType: ColumnDataType;
  materializationId: string;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrgId?: string;
  isIdentity?: boolean;
  isNullable?: boolean;
  comment?: string;
}

export type CreateColumnAuthDto = BaseAuth;

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
    req: CreateColumnRequestDto,
    auth: CreateColumnAuthDto,
    connPool: IConnectionPool
  ): Promise<CreateColumnResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const column = Column.create({
        id: uuidv4(),
        relationName: req.relationName,
        name: req.name,
        index: req.index,
        dataType: req.dataType,
        materializationId: req.materializationId,
        lineageId: req.lineageId,
        isIdentity: req.isIdentity,
        isNullable: req.isNullable,
        comment: req.comment,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: req.name,
          materializationId: req.materializationId,
          lineageId: req.lineageId,
          targetOrgId: req.targetOrgId,
        },
        auth,
        connPool
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for materialization already exists`);

      if (req.writeToPersistence)
        await this.#columnRepo.insertOne(
          column,
          auth,
          connPool,
          req.targetOrgId
        );

      return Result.ok(column);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
