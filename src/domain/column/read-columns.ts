import { Column } from '../entities/column';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import Result from '../value-types/transient-types/result';
import { IColumnRepo, ColumnQueryDto } from './i-column-repo';

export interface ReadColumnsRequestDto {
  relationName?: string | string[];
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
  targetOrgId?: string;
}

export type ReadColumnsAuthDto = BaseAuth;

export type ReadColumnsResponseDto = Result<Column[]>;

export class ReadColumns
  implements
    IUseCase<ReadColumnsRequestDto, ReadColumnsResponseDto, ReadColumnsAuthDto>
{
  readonly #columnRepo: IColumnRepo;

  constructor(
    columnRepo: IColumnRepo,
  ) {
    this.#columnRepo = columnRepo;
  }

  async execute(
    req: ReadColumnsRequestDto,
    auth: ReadColumnsAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadColumnsResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const columns: Column[] = await this.#columnRepo.findBy(
        this.#buildColumnQueryDto(req),
        auth,
        connPool,
        req.targetOrgId
      );
      if (!columns) throw new Error(`Queried columns do not exist`);

      return Result.ok(columns);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildColumnQueryDto = (request: ReadColumnsRequestDto): ColumnQueryDto => {
    const queryDto: ColumnQueryDto = {
      lineageId: request.lineageId,
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
