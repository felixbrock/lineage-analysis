import Result from '../value-types/transient-types/result';
import {} from '../services/i-db';
import {
  ConnectionPool,
  ISnowflakeApiRepo,
  SnowflakeQueryResult,
} from './i-snowflake-api-repo';
import BaseSfQueryUseCase from '../services/base-sf-query-use-case';
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

export class QuerySnowflake extends BaseSfQueryUseCase<
  QuerySnowflakeRequestDto,
  QuerySnowflakeResponseDto,
  QuerySnowflakeAuthDto
> {
  readonly #repo: ISnowflakeApiRepo;

  constructor(repo: ISnowflakeApiRepo, getProfile: GetSnowflakeProfile) {
    super(getProfile);
    this.#repo = repo;
  }

  async execute(
    request: QuerySnowflakeRequestDto,
    auth: QuerySnowflakeAuthDto,
    connPool: ConnectionPool
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
      const queryResult = await this.#repo.runQuery(
        request.queryText,
        request.binds,
        connPool
      );

      const stringifiedBinds = JSON.stringify(request.binds);

      if (!queryResult.success) {
        const queryResultBaseMsg = `Binds: ${stringifiedBinds.substring(
          0,
          1000
        )}${stringifiedBinds.length > 1000 ? '...' : ''}
          \n${request.queryText.substring(0, 1000)}${
          request.queryText.length > 1000 ? '...' : ''
        }`;

        throw new Error(
          `Sf query failed \n${queryResultBaseMsg} \nError msg: ${queryResult.error}`
        );
      }

      return Result.ok(queryResult.value);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
