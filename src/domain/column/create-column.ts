import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column } from '../entities/column';
import { ReadColumns } from './read-columns';
import { IColumnRepo } from './i-column-repo';
import { DbConnection } from '../services/i-db';

export interface CreateColumnRequestDto {
  relationName: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageIds: string[];
  writeToPersistence: boolean;
  targetOrganizationId?: string;
}

export interface CreateColumnAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
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
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
        if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId: string;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');

      this.#dbConnection = dbConnection;

      const column = Column.create({
        id: new ObjectId().toHexString(),
        relationName: request.relationName,
        name: request.name,
        index: request.index,
        type: request.type,
        materializationId: request.materializationId,
        lineageIds: request.lineageIds,
        organizationId,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: request.name,
          materializationId: request.materializationId,
          lineageIds: request.lineageIds,
          targetOrganizationId: request.targetOrganizationId,
        },
        { isSystemInternal: auth.isSystemInternal, callerOrganizationId: auth.callerOrganizationId },
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
      if((error instanceof Error && error.message) || (!(error instanceof Error) && error)) if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
