import { Column } from '../entities/column';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IColumnRepo, ColumnQueryDto } from './i-column-repo';

export interface ReadColumnsRequestDto {
  dbtModelId?: string | string[],
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
}

export interface ReadColumnsAuthDto {
  organizationId: string;
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
      const columns: Column[] = await this.#columnRepo.findBy(
        this.#buildColumnQueryDto(request, auth.organizationId)
      );
      if (!columns) throw new Error(`Queried columns do not exist`);

      return Result.ok(columns);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildColumnQueryDto = (
    request: ReadColumnsRequestDto,
    organizationId: string
  ): ColumnQueryDto => {
    console.log(organizationId);

    const queryDto: ColumnQueryDto = { lineageId: request.lineageId };

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.dbtModelId) queryDto.dbtModelId = request.dbtModelId;
    if (request.name) queryDto.name = request.name;
    if (request.index) queryDto.index  = request.index;
    if (request.type) queryDto.type = request.type;
    if (request.materializationId) queryDto.materializationId = request.materializationId;
   
    return queryDto;
  };
}
