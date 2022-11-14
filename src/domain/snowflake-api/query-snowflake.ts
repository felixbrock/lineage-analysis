import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {} from '../services/i-db';
import {
  ISnowflakeApiRepo,
  SnowflakeQueryResult,
} from './i-snowflake-api-repo';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';

export interface QuerySnowflakeRequestDto {
  queryText: string;
  binds: (string | number)[] | (string | number)[][];
  targetOrgId?: string;
}

export interface QuerySnowflakeAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
}

export type QuerySnowflakeResponseDto = Result<SnowflakeQueryResult>;

export class QuerySnowflake
  implements
    IUseCase<
      QuerySnowflakeRequestDto,
      QuerySnowflakeResponseDto,
      QuerySnowflakeAuthDto
    >
{
  readonly #snowflakeApiRepo: ISnowflakeApiRepo;

  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(
    snowflakeApiRepo: ISnowflakeApiRepo,
    getSnowflakeProfile: GetSnowflakeProfile
  ) {
    this.#snowflakeApiRepo = snowflakeApiRepo;
    this.#getSnowflakeProfile = getSnowflakeProfile;
  }

  #getProfile = async (
    jwt: string,
    targetOrgId?: string,
  ): Promise<SnowflakeProfileDto> => {
    const readSnowflakeProfileResult = await this.#getSnowflakeProfile.execute(
      {targetOrgId},
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
    request: QuerySnowflakeRequestDto,
    auth: QuerySnowflakeAuthDto
  ): Promise<QuerySnowflakeResponseDto> {
    if (auth.isSystemInternal && !request.targetOrgId)
      throw new Error('Target organization id missing');
    if (!auth.isSystemInternal && !auth.callerOrgId)
      throw new Error('Caller organization id missing');
    if (!request.targetOrgId && !auth.callerOrgId)
      throw new Error('No organization Id instance provided');
    if (request.targetOrgId && auth.callerOrgId)
      throw new Error('callerOrgId and targetOrgId provided. Not allowed');

    try {
      const profile: SnowflakeProfileDto = await this.#getProfile(
        auth.jwt,
        auth.isSystemInternal ? request.targetOrgId: undefined,
      );

      const queryResult = await this.#snowflakeApiRepo.runQuery(
        request.queryText,
        request.binds,
        {
          account: profile.accountId,
          username: profile.username,
          password: profile.password,
          warehouse: profile.warehouseName,
        }
      );

      const stringifiedBinds = JSON.stringify(request.binds);

      const queryResultBaseMsg = `AcccountId: ${
        profile.accountId
      } \nOrganizationId: ${
        profile.organizationId
      } \n Binds: ${stringifiedBinds.substring(0, 1000)}${
        stringifiedBinds.length > 1000 ? '...' : ''
      }
      \n${request.queryText.substring(0, 1000)}${
        request.queryText.length > 1000 ? '...' : ''
      }`;

      if (!queryResult.success)
        throw new Error(
          `Sf query failed \n${queryResultBaseMsg} \nError msg: ${queryResult.error}`
        );
      else console.log(`Sf query succeeded \n${queryResultBaseMsg}`);

      // const value =
      //   queryResult.success && queryResult.value
      //     ? JSON.parse(
      //         JSON.stringify(queryResult.value).replace(
      //           /[", ']null[", ']/g,
      //           'null'
      //         )
      //       )
      //     : [];

      return Result.ok(queryResult.value);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
