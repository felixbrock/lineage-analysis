import { Column } from '../entities/column';
import {} from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IColumnRepo, ColumnQueryDto } from './i-column-repo';

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
  jwt:string
}

export type ReadColumnsResponseDto = Result<Column[]>;

export class ReadColumns
  implements
    IUseCase<ReadColumnsRequestDto, ReadColumnsResponseDto, ReadColumnsAuthDto>
{
  readonly #columnRepo: IColumnRepo;

  constructor(columnRepo: IColumnRepo) {
    this.#columnRepo = columnRepo;
  }

  async execute(
    request: ReadColumnsRequestDto,
    auth: ReadColumnsAuthDto
  ): Promise<ReadColumnsResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const columns: Column[] = await this.#columnRepo.findBy(
        this.#buildColumnQueryDto(request),
        auth,
        request.targetOrganizationId
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
