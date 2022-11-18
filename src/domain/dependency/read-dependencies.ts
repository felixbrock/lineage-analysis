import { Dependency, DependencyType } from '../entities/dependency';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import Result from '../value-types/transient-types/result';
import { IDependencyRepo, DependencyQueryDto } from './i-dependency-repo';

export interface ReadDependenciesRequestDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
  targetOrgId?: string;
  
}

export type ReadDependenciesAuthDto = BaseAuth


export type ReadDependenciesResponseDto = Result<Dependency[]>;

export class ReadDependencies
  implements
    IUseCase<
      ReadDependenciesRequestDto,
      ReadDependenciesResponseDto,
      ReadDependenciesAuthDto
    >
{
  readonly #dependencyRepo: IDependencyRepo;

  constructor(
    dependencyRepo: IDependencyRepo,
  ) {
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    req: ReadDependenciesRequestDto,
    auth: ReadDependenciesAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadDependenciesResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

            const dependencies: Dependency[] = await this.#dependencyRepo.findBy(
        this.#buildDependencyQueryDto(req),
        auth,
        connPool,
        req.targetOrgId
      );
      if (!dependencies)
        throw new ReferenceError(`Queried dependencies do not exist`);

      return Result.ok(dependencies);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildDependencyQueryDto = (
    request: ReadDependenciesRequestDto
  ): DependencyQueryDto => {
    const queryDto: DependencyQueryDto = { lineageId: request.lineageId };

    if (request.type) queryDto.type = request.type;
    if (request.headId) queryDto.headId = request.headId;
    if (request.tailId) queryDto.tailId = request.tailId;

    return queryDto;
  };
}
