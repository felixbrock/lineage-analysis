import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column, ColumnDataType } from '../entities/column';
import { ReadColumns } from './read-columns';
import { IColumnRepo } from './i-column-repo';

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

export interface CreateColumnAuthDto {
  isSystemInternal: boolean;
  callerOrgId?: string;
  jwt:string;
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
      if (auth.isSystemInternal && !request.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const column = Column.create({
        id: uuidv4(),
        relationName: request.relationName,
        name: request.name,
        index: request.index,
        dataType: request.dataType,
        materializationId: request.materializationId,
        lineageId: request.lineageId,
        isIdentity: request.isIdentity,
        isNullable: request.isNullable,
        comment: request.comment,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: request.name,
          materializationId: request.materializationId,
          lineageId: request.lineageId,
          targetOrgId: request.targetOrgId,
        },
        auth
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for materialization already exists`);

      if (request.writeToPersistence)
        await this.#columnRepo.insertOne(
          column,
          auth,
          request.targetOrgId
        );

      return Result.ok(column);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
