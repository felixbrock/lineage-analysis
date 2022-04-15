import { Table } from '../entities/table';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ITableRepo, TableQueryDto } from './i-table-repo';

export interface ReadTablesRequestDto {
  dbtModelId?: string;
  name?: string | string[];
  modelId?: string;
  lineageId: string;
}

export interface ReadTablesAuthDto {
  organizationId: string;
}

export type ReadTablesResponseDto = Result<Table[]>;

export class ReadTables
  implements
    IUseCase<ReadTablesRequestDto, ReadTablesResponseDto, ReadTablesAuthDto>
{
  readonly #tableRepo: ITableRepo;

  constructor(tableRepo: ITableRepo) {
    this.#tableRepo = tableRepo;
  }

  async execute(
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
    console.log(organizationId);

    const queryDto: TableQueryDto = {lineageId: request.lineageId};

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.dbtModelId) queryDto.dbtModelId = request.dbtModelId;
    if (request.name) queryDto.name = request.name;
    if (request.modelId) queryDto.modelId = request.modelId;

    return queryDto;
  };
}
