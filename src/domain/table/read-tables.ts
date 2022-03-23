import { Table } from '../entities/table';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ITableRepo, TableQueryDto } from './i-table-repo';
import { TableDto, buildTableDto } from './table-dto';

export interface ReadTablesRequestDto {
  name?: string;
  modelId?: string;
}

export interface ReadTablesAuthDto {
  organizationId: string;
}

export type ReadTablesResponseDto = Result<Table[]>;

export class ReadTables
  implements
    IUseCase<ReadTablesRequestDto, ReadTablesResponseDto, ReadTablesAuthDto>
{
  #tableRepo: ITableRepo;

  public constructor(tableRepo: ITableRepo) {
    this.#tableRepo = tableRepo;
  }

  public async execute(
    request: ReadTablesRequestDto,
    auth: ReadTablesAuthDto
  ): Promise<ReadTablesResponseDto> {
    try {
      const tables: Table[] = await this.#tableRepo.findBy(
        this.#buildTableQueryDto(request, auth.organizationId)
      );
      if (!tables) throw new Error(`Queried tables do not exist`);

      return Result.ok(tables);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildTableQueryDto = (
    request: ReadTablesRequestDto,
    organizationId: string
  ): TableQueryDto => {
    const queryDto: TableQueryDto = {};

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.name) queryDto.name = request.name;
    if (request.modelId) queryDto.modelId = request.modelId;

    return queryDto;
  };
}
