import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import BaseAuth from '../services/base-auth';
import { IDbConnection } from '../services/i-db';

export interface ReadLineageRequestDto {
  id?: string;
  targetOrgId?: string;

  tolerateIncomplete: boolean;
  minuteTolerance?: number;
}

export type ReadLineageAuthDto = BaseAuth;

export type ReadLineageResponseDto = Result<Lineage | undefined>;

export class ReadLineage
  implements
    IUseCase<
      ReadLineageRequestDto,
      ReadLineageResponseDto,
      ReadLineageAuthDto,
      IDbConnection
    >
{
  readonly #lineageRepo: ILineageRepo;

  constructor(lineageRepo: ILineageRepo) {
    this.#lineageRepo = lineageRepo;
  }

  async execute(
    req: ReadLineageRequestDto,
    auth: ReadLineageAuthDto,
    dbConnection: IDbConnection
  ): Promise<ReadLineageResponseDto> {
    try {
      const lineage = req.id
        ? await this.#lineageRepo.findOne(req.id, auth, dbConnection)
        : await this.#lineageRepo.findLatest(
            {
              tolerateIncomplete: req.tolerateIncomplete,
              minuteTolerance: req.minuteTolerance,
            },
            auth,
            dbConnection,
            req.targetOrgId
          );
      if (req.id && !lineage)
        throw new Error(
          `No lineage found for organization ${auth.callerOrgId}`
        );

      return Result.ok(lineage);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
