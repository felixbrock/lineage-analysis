import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';

export interface ReadLineageRequestDto {
  id: string;
}

export interface ReadLineageAuthDto {
  organizationId: string;
}

export type ReadLineageResponseDto = Result<Lineage>;

export class ReadLineage
  implements
    IUseCase<ReadLineageRequestDto, ReadLineageResponseDto, ReadLineageAuthDto>
{
  readonly #lineageRepo: ILineageRepo;

  constructor(lineageRepo: ILineageRepo) {
    this.#lineageRepo = lineageRepo;
  }

  async execute(
    request: ReadLineageRequestDto,
    auth: ReadLineageAuthDto
  ): Promise<ReadLineageResponseDto> {
    try {
      // todo -replace
      console.log(auth);

      const lineage = await this.#lineageRepo.findOne(request.id);
      if (!lineage) throw new Error(`Lineage with id ${request.id} does not exist`);

      // if (lineage.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      return Result.ok(lineage);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
