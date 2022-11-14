import { Logic } from '../entities/logic';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import {} from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ILogicRepo, LogicQueryDto } from './i-logic-repo';

export interface ReadLogicsRequestDto {
  relationName?: string;
  lineageId: string;
  targetOrgId?: string;
  profile?: SnowflakeProfileDto;
}

export interface ReadLogicsAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
}

export type ReadLogicsResponseDto = Result<Logic[]>;

export class ReadLogics
  implements
    IUseCase<ReadLogicsRequestDto, ReadLogicsResponseDto, ReadLogicsAuthDto>
{
  readonly #logicRepo: ILogicRepo;

  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(logicRepo: ILogicRepo, getSnowflakeProfile: GetSnowflakeProfile) {
    this.#logicRepo = logicRepo;
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
    request: ReadLogicsRequestDto,
    auth: ReadLogicsAuthDto
  ): Promise<ReadLogicsResponseDto> {
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

      const logics: Logic[] = await this.#logicRepo.findBy(
        this.#buildLogicQueryDto(request),
        profile,
        auth,
        request.targetOrgId
      );
      if (!logics) throw new Error(`Queried logics do not exist`);

      return Result.ok(logics);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildLogicQueryDto = (request: ReadLogicsRequestDto): LogicQueryDto => {
    const queryDto: LogicQueryDto = {
      lineageId: request.lineageId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;

    return queryDto;
  };
}
