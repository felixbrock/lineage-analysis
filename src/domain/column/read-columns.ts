import { Column } from '../entities/column';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ILegacyColumnRepo, ColumnQueryDto } from './i-column-repo';

export interface ReadColumnsRequestDto {
  relationName?: string | string[];
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
  targetOrganizationId?: string;
}

export interface ReadColumnsAuthDto {
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export type ReadColumnsResponseDto = Result<Column[]>;

export class ReadColumns
  implements
    IUseCase<
      ReadColumnsRequestDto,
      ReadColumnsResponseDto,
      ReadColumnsAuthDto,
      DbConnection
    >
{
  readonly #columnRepo: ILegacyColumnRepo;

  #dbConnection: DbConnection;

  constructor(columnRepo: ILegacyColumnRepo) {
    this.#columnRepo = columnRepo;
  }

  async execute(
    request: ReadColumnsRequestDto,
    auth: ReadColumnsAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadColumnsResponseDto> {
    try {
      this.#dbConnection = dbConnection;

      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organizationId allocation');

      const columns: Column[] = await this.#columnRepo.findBy(
        this.#buildColumnQueryDto(request, organizationId),
        this.#dbConnection
      );
      if (!columns) throw new Error(`Queried columns do not exist`);

      return Result.ok(columns);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildColumnQueryDto = (
    request: ReadColumnsRequestDto,
    organizationId: string
  ): ColumnQueryDto => {
    const queryDto: ColumnQueryDto = {
      lineageId: request.lineageId,
      organizationId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;
    if (request.name) queryDto.name = request.name;
    if (request.index) queryDto.index = request.index;
    if (request.type) queryDto.type = request.type;
    if (request.materializationId)
      queryDto.materializationId = request.materializationId;

    return queryDto;
  };
}
