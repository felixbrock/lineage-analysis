import { Dependency, DependencyType } from '../entities/dependency';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IDependencyRepo, DependencyQueryDto } from './i-dependency-repo';

export interface ReadDependenciesRequestDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
  targetOrganizationId?: string;
}

export interface ReadDependenciesAuthDto {
  callerOrganizationId: string;
  isSystemInternal: boolean
}

export type ReadDependenciesResponseDto = Result<Dependency[]>;

export class ReadDependencies
  implements
    IUseCase<
      ReadDependenciesRequestDto,
      ReadDependenciesResponseDto,
      ReadDependenciesAuthDto,
      DbConnection
    >
{
  readonly #dependencyRepo: IDependencyRepo;

  #dbConnection: DbConnection;

  constructor(dependencyRepo: IDependencyRepo) {
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: ReadDependenciesRequestDto,
    auth: ReadDependenciesAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadDependenciesResponseDto> {
    try {
      if(auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');

      this.#dbConnection = dbConnection;

      const organizationId = auth.isSystemInternal && request.targetOrganizationId ? request.targetOrganizationId: auth.callerOrganizationId;

      const dependencies: Dependency[] = await this.#dependencyRepo.findBy(
        this.#buildDependencyQueryDto(request, organizationId),
        dbConnection
      );
      if (!dependencies)
        throw new ReferenceError(`Queried dependencies do not exist`);

      return Result.ok(dependencies);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildDependencyQueryDto = (
    request: ReadDependenciesRequestDto,
    organizationId: string
  ): DependencyQueryDto => {
    

    const queryDto: DependencyQueryDto = { lineageId: request.lineageId, organizationId };

    
    
    if (request.type) queryDto.type = request.type;
    if (request.headId) queryDto.headId = request.headId;
    if (request.tailId) queryDto.tailId = request.tailId;

    return queryDto;
  };
}
