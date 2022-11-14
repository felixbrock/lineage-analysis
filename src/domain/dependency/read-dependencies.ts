import { Dependency, DependencyType } from '../entities/dependency';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import {} from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IDependencyRepo, DependencyQueryDto } from './i-dependency-repo';

export interface ReadDependenciesRequestDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
  targetOrgId?: string;
  profile?: SnowflakeProfileDto;
}

export interface ReadDependenciesAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
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

  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(
    dependencyRepo: IDependencyRepo,
    getSnowflakeProfile: GetSnowflakeProfile
  ) {
    this.#dependencyRepo = dependencyRepo;
    this.#getSnowflakeProfile = getSnowflakeProfile;
  }

  #getProfile = async (
    jwt: string,
    targetOrgId?: string
  ): Promise<SnowflakeProfileDto> => {
    const readSnowflakeProfileResult = await this.#getSnowflakeProfile.execute(
      { targetOrgId },
      {
        jwt,
      }
    );

    if (!readSnowflakeProfileResult.success)
      throw new Error(readSnowflakeProfileResult.error);
    if (!readSnowflakeProfileResult.value)
      throw new Error('SnowflakeProfile does not exist');

    return readSnowflakeProfileResult.value;
  };

  async execute(
    request: ReadDependenciesRequestDto,
    auth: ReadDependenciesAuthDto
  ): Promise<ReadDependenciesResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const profile =
        request.profile ||
        (await this.#getProfile(
          auth.jwt,
          auth.isSystemInternal ? request.targetOrgId : undefined
        ));

      const dependencies: Dependency[] = await this.#dependencyRepo.findBy(
        this.#buildDependencyQueryDto(request),
        profile,
        auth,
        request.targetOrgId
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
