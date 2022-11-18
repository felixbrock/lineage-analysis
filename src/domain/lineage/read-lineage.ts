import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

export interface ReadLineageRequestDto {
  id?: string;
  targetOrgId?: string;
  
  tolerateIncomplete: boolean;
  minuteTolerance?: number;
}

export type ReadLineageAuthDto = BaseAuth;

export type ReadLineageResponseDto = Result<Lineage | null>;

export class ReadLineage
  implements
    IUseCase<ReadLineageRequestDto, ReadLineageResponseDto, ReadLineageAuthDto>
{
  readonly #lineageRepo: ILineageRepo;

  constructor(
    lineageRepo: ILineageRepo,
  ) {
    this.#lineageRepo = lineageRepo;
  }

  async execute(
    req: ReadLineageRequestDto,
    auth: ReadLineageAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadLineageResponseDto> {
    try {
      const lineage = req.id
        ? await this.#lineageRepo.findOne(
            req.id,
            auth,
            connPool,
            req.targetOrgId
          )
        : await this.#lineageRepo.findLatest(
            {
              tolerateIncomplete: req.tolerateIncomplete,
              minuteTolerance: req.minuteTolerance,
            },
            auth,
            connPool,
            req.targetOrgId
          );
      if (req.id && !lineage)
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
