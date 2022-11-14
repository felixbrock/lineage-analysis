import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import {} from '../services/i-db';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';

export interface ReadLineageRequestDto {
  id?: string;
  targetOrgId?: string;
  profile?: SnowflakeProfileDto;
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

  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(
    lineageRepo: ILineageRepo,
    getSnowflakeProfile: GetSnowflakeProfile
  ) {
    this.#lineageRepo = lineageRepo;
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
    request: ReadLineageRequestDto,
    auth: ReadLineageAuthDto
  ): Promise<ReadLineageResponseDto> {
    try {
      const profile =
        request.profile ||
        (await this.#getProfile(
          auth.jwt,
          auth.isSystemInternal ? request.targetOrgId : undefined
        ));

      const lineage = request.id
        ? await this.#lineageRepo.findOne(
            request.id,
            profile,
            auth,
            request.targetOrgId
          )
        : await this.#lineageRepo.findLatest(
            {
              completed: true,
            },
            profile,
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
