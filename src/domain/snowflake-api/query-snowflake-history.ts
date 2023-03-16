import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { BiToolType } from '../value-types/bi-tool';
import {
  Binds,
  IConnectionPool,
  SnowflakeQueryResult,
} from './i-snowflake-api-repo';
import { QuerySnowflake } from './query-snowflake';
import BaseAuth from '../services/base-auth';

export interface QuerySfQueryHistoryRequestDto {
  biType: BiToolType;

  targetOrgId?: string;
}

export type QuerySfQueryHistoryAuthDto = BaseAuth;

export type QuerySfQueryHistoryResponseDto = Result<SnowflakeQueryResult>;

export class QuerySfQueryHistory
  implements
    IUseCase<
      QuerySfQueryHistoryRequestDto,
      QuerySfQueryHistoryResponseDto,
      QuerySfQueryHistoryAuthDto,
      IConnectionPool
    >
{
  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  static #buildQuery = (biType: BiToolType): string => {
    let condition = '';
    switch (biType) {
      case 'Tableau': {
        condition = `query_tag ilike '%tableau%'`;
        break;
      }
      default: {
        throw new Error('Invalid BI tool type');
      }
    }

    return `select query_text, query_tag from snowflake.account_usage.query_history
     where ${condition} and start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())`;
  };

  async execute(
    request: QuerySfQueryHistoryRequestDto,
    auth: QuerySfQueryHistoryAuthDto,
    connPool: IConnectionPool
  ): Promise<QuerySfQueryHistoryResponseDto> {
    if (!request.targetOrgId && !auth.callerOrgId)
      throw new Error('No organization Id instance provided');
    if (request.targetOrgId && auth.callerOrgId)
      throw new Error('callerOrgId and targetOrgId provided. Not allowed');

    try {
      const binds: Binds = [];
      const queryText = QuerySfQueryHistory.#buildQuery(request.biType);

      const queryResult = await this.#querySnowflake.execute(
        {
          queryText,
          binds,
        },
        auth,
        connPool
      );

      if (!queryResult.success) throw new Error(queryResult.error);
      if (!queryResult.value) throw new Error('Query result is missing value');

      return Result.ok(queryResult.value);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
