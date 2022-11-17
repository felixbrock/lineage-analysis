import { createPool } from 'snowflake-sdk';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
import { ConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { DbOptions } from './i-db';
import IUseCase from './use-case';

export default abstract class BaseSfQueryUseCase<ReqDto, ResDto, AuthDto>
  implements IUseCase<ReqDto, ResDto, AuthDto>
{
  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(getSnowflakeProfile: GetSnowflakeProfile) {
    this.#getSnowflakeProfile = getSnowflakeProfile;
  }

  abstract execute(
    request: ReqDto,
    auth: AuthDto,
    connectionPool?: ConnectionPool
  ): ResDto | Promise<ResDto>;
}
