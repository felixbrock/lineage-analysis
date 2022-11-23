import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column, ColumnDataType } from '../entities/column';
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
      ,IConnectionPool
    >
{
  readonly #columnRepo: IColumnRepo;


  constructor(columnRepo: IColumnRepo) {
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

      if (req.writeToPersistence)
        await this.#columnRepo.insertOne(
          column,
          auth,
          connPool,
        );

      return Result.ok(column);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.error(error.stack);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
