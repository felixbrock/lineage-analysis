import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ITableRepo } from './i-table-repo';
import { buildTableDto, TableDto } from './table-dto';

export interface ReadTableRequestDto {
  id: string;
}

export interface ReadTableAuthDto {
  organizationId: string;
}

export type ReadTableResponseDto = Result<TableDto>;

export class ReadTable
  implements
    IUseCase<ReadTableRequestDto, ReadTableResponseDto, ReadTableAuthDto>
{
  readonly #tableRepo: ITableRepo;

  constructor(tableRepo: ITableRepo) {
    this.#tableRepo = tableRepo;
  }

  async execute(
    request: ReadTableRequestDto,
    auth: ReadTableAuthDto
  ): Promise<ReadTableResponseDto> {
    console.log(auth);

    try {
      const table = await this.#tableRepo.findOne(request.id);
      if (!table) throw new Error(`Table with id ${request.id} does not exist`);

      // if (table.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      return Result.ok(buildTableDto(table));
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
