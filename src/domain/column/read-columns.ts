import { Column } from '../entities/column';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IColumnRepo, ColumnQueryDto } from './i-column-repo';
import { IDbConnection } from '../services/i-db';

export interface ReadColumnsRequestDto {
  relationNames?: string[];
  names?: string[];
  index?: string;
  type?: string;
  materializationIds?: string[];
  targetOrgId?: string;
}

export type ReadColumnsAuthDto = BaseAuth;

export type ReadColumnsResponseDto = Result<Column[]>;

export class ReadColumns
  implements
    IUseCase<ReadColumnsRequestDto, ReadColumnsResponseDto, ReadColumnsAuthDto, IDbConnection>
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
    dbConnection: IDbConnection
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
        dbConnection,
      );
      if (!columns) throw new Error(`Queried columns do not exist`);

      return Result.ok(columns);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildColumnQueryDto = (request: ReadColumnsRequestDto): ColumnQueryDto => {
    const queryDto: ColumnQueryDto = {
    };

    if (request.relationNames) queryDto.relationNames = request.relationNames;
    if (request.names) queryDto.names = request.names;
    if (request.index) queryDto.index = request.index;
    if (request.type) queryDto.type = request.type;
    if (request.materializationIds)
      queryDto.materializationIds = request.materializationIds;

    return queryDto;
  };
}
