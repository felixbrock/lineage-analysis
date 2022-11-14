import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import {} from '../services/i-db';

export interface ReadLineageRequestDto {
  id?: string;
  targetOrgId?: string;
}

export interface ReadLineageAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
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
      const lineage = request.id
        ? await this.#lineageRepo.findOne(request.id, auth, request.targetOrgId)
        : await this.#lineageRepo.findLatest(
            {
              completed: true,
            },
            auth,
            request.targetOrgId
          );
      if (!lineage)
        throw new Error(
          `No lineage found for organization ${auth.callerOrgId}`
        );

      return Result.ok(lineage);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
