import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import { DbConnection } from '../services/i-db';

export interface ReadLineageRequestDto {
  id?: string;
}

export interface ReadLineageAuthDto {
  callerOrganizationId: string;
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
      this.#dbConnection = dbConnection;

      const lineage = request.id
        ? await this.#lineageRepo.findOne(this.#dbConnection, request.id)
        : await this.#lineageRepo.findLatest(this.#dbConnection, {
            organizationId: auth.callerOrganizationId, completed: true
          });
      if (!lineage)
        throw new Error(
          `No lineage found for organization ${auth.callerOrganizationId}`
        );

      if (lineage.organizationId !== auth.callerOrganizationId)
        throw new Error('Not authorized to perform action');

      return Result.ok(lineage);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
