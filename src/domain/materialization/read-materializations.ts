import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import {} from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import {
  IMaterializationRepo,
  MaterializationQueryDto,
} from './i-materialization-repo';

export interface ReadMaterializationsRequestDto {
  relationName?: string;
  materializationType?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
  targetOrgId?: string;
  profile?: SnowflakeProfileDto;
}

export interface ReadMaterializationsAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
}

export type ReadMaterializationsResponseDto = Result<Materialization[]>;

export class ReadMaterializations
  implements
    IUseCase<
      ReadMaterializationsRequestDto,
      ReadMaterializationsResponseDto,
      ReadMaterializationsAuthDto
    >
{
  readonly #materializationRepo: IMaterializationRepo;

  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(
    materializationRepo: IMaterializationRepo,
    getSnowflakeProfile: GetSnowflakeProfile
  ) {
    this.#materializationRepo = materializationRepo;
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
    request: ReadMaterializationsRequestDto,
    auth: ReadMaterializationsAuthDto
  ): Promise<ReadMaterializationsResponseDto> {
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

      const materializations: Materialization[] =
        await this.#materializationRepo.findBy(
          this.#buildMaterializationQueryDto(request),
          profile,
          auth,
          request.targetOrgId
        );
      if (!materializations)
        throw new Error(`Queried materializations do not exist`);

      return Result.ok(materializations);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildMaterializationQueryDto = (
    request: ReadMaterializationsRequestDto
  ): MaterializationQueryDto => {
    const queryDto: MaterializationQueryDto = {
      lineageId: request.lineageId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;
    if (request.name) queryDto.name = request.name;
    if (request.logicId) queryDto.logicId = request.logicId;

    return queryDto;
  };
}
