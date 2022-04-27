import { Dependency, DependencyType } from '../entities/dependency';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IDependencyRepo, DependencyQueryDto } from './i-dependency-repo';

export interface ReadDependenciesRequestDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
}

export interface ReadDependenciesAuthDto {
  organizationId: string;
}

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

  constructor(dependencyRepo: IDependencyRepo) {
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: ReadDependenciesRequestDto,
    auth: ReadDependenciesAuthDto
  ): Promise<ReadDependenciesResponseDto> {
    try {
      const dependencies: Dependency[] = await this.#dependencyRepo.findBy(
        this.#buildDependencyQueryDto(request, auth.organizationId)
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
    console.log(organizationId);

    const queryDto: DependencyQueryDto = { lineageId: request.lineageId };

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.type) queryDto.type = request.type;
    if (request.headId) queryDto.headId = request.headId;
    if (request.tailId) queryDto.tailId = request.tailId;

    return queryDto;
  };
}
