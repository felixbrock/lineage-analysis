import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import { DbConnection } from '../services/i-db';

export interface ReadLineageRequestDto {
  id: string;
}

export interface ReadLineageAuthDto {
  organizationId: string;
}

export type ReadLineageResponseDto = Result<Lineage>;

export class ReadLineage
  implements
    IUseCase<
      ReadLineageRequestDto,
      ReadLineageResponseDto,
      ReadLineageAuthDto,
      DbConnection
    >
{
  readonly #lineageRepo: ILineageRepo;

  #dbConnection: DbConnection;

  constructor(lineageRepo: ILineageRepo) {
    this.#lineageRepo = lineageRepo;
  }

  async execute(
    request: ReadLineageRequestDto,
    auth: ReadLineageAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadLineageResponseDto> {
    try {
      // todo -replace
      console.log(auth);

      this.#dbConnection = dbConnection;

      const lineage = await this.#lineageRepo.findOne(
        request.id,
        this.#dbConnection
      );
      if (!lineage)
        throw new Error(`Lineage with id ${request.id} does not exist`);

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
