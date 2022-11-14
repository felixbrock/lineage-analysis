import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILogicRepo } from './i-logic-repo';
import { Logic } from '../entities/logic';
import {} from '../services/i-db';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';

export interface ReadLogicRequestDto {
  id: string;
  targetOrgId?: string;
  profile?: SnowflakeProfileDto;
}

export interface ReadLogicAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
}

export type ReadLogicResponseDto = Result<Logic>;

export class ReadLogic
  implements
    IUseCase<ReadLogicRequestDto, ReadLogicResponseDto, ReadLogicAuthDto>
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
    request: ReadLogicRequestDto,
    auth: ReadLogicAuthDto
  ): Promise<ReadLogicResponseDto> {
    try {
      const profile =
        request.profile ||
        (await this.#getProfile(
          auth.jwt,
          auth.isSystemInternal ? request.targetOrgId : undefined
        ));

      const logic = await this.#logicRepo.findOne(
        request.id,
        profile,
        auth,
        request.targetOrgId
      );
      if (!logic) throw new Error(`Logic with id ${request.id} does not exist`);

      return Result.ok(logic);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
