import { Column } from '../entities/column';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IColumnRepo, ColumnQueryDto } from './i-column-repo';
import { ColumnDto, buildColumnDto } from './column-dto';

export interface ReadColumnsRequestDto {
  name?: string | string[];
  tableId?: string | string[];
  dependency?: { type?: string; columnId?: string; direction?: string };
}

export interface ReadColumnsAuthDto {
  organizationId: string;
}

export type ReadColumnsResponseDto = Result<Column[]>;

export class ReadColumns
  implements
    IUseCase<ReadColumnsRequestDto, ReadColumnsResponseDto, ReadColumnsAuthDto>
{
  #columnRepo: IColumnRepo;

  public constructor(columnRepo: IColumnRepo) {
    this.#columnRepo = columnRepo;
  }

  public async execute(
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
    const queryDto: ColumnQueryDto = {};

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.name) queryDto.name = request.name;
    if (request.tableId) queryDto.tableId = request.tableId;
    if (
      request.dependency &&
      (request.dependency.type ||
        request.dependency.columnId ||
        request.dependency.direction)
    )
      queryDto.dependency = request.dependency;

    return queryDto;
  };
}
