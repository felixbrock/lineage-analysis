import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { BiToolType } from '../value-types/bi-tool';
import { IConnectionPool, SnowflakeQueryResult } from './i-snowflake-api-repo';
import { QuerySnowflake } from './query-snowflake';
import BaseAuth from '../services/base-auth';

export interface QuerySfQueryHistoryRequestDto {
  biType: BiToolType;
  limit: number;

  targetOrgId?: string;
}

export type QuerySfQueryHistoryAuthDto = BaseAuth;

export type QuerySfQueryHistoryResponseDto = Result<SnowflakeQueryResult>;

export class QuerySfQueryHistory
  implements
    IUseCase<
      QuerySfQueryHistoryRequestDto,
      QuerySfQueryHistoryResponseDto,
      QuerySfQueryHistoryAuthDto,IConnectionPool
    >
{
  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  static #buildQuery = (biType: BiToolType): string => {
    let condition = '';
    let regex = '';
    switch (biType) {
      case 'Mode': {
        // eslint-disable-next-line no-useless-escape
        regex = 'https://modeanalytics.com[^s"]+';
        condition = `REGEXP_COUNT(QUERY_TEXT,'${regex}') > 0
        limit ?`;
        break;
      }
      case 'Tableau': {
        // eslint-disable-next-line no-useless-escape
        regex = '"TableauSQL"';
        condition = `REGEXP_COUNT(QUERY_TEXT,'${regex}') > 0
         limit ?`;
        break;
      }
      case 'Metabase': {
        // eslint-disable-next-line no-useless-escape
        regex = '("[A-Za-z0-9_$]+".){2}("[A-Za-z0-9_$]+")';
        condition = `REGEXP_COUNT(QUERY_TEXT,'FROM ${regex}') > 0
        AND REGEXP_COUNT(QUERY_TEXT,'${regex} AS') > 0
        AND CHARINDEX('WHERE 1 <> 1 LIMIT 0', QUERY_TEXT) = 0
        AND CHARINDEX('source', QUERY_TEXT) = 0
        AND WAREHOUSE_ID IS NOT NULL
        limit ?`;
        break;
      }
      default: {
        condition = 'false';
        break;
      }
    }

    return `select QUERY_TEXT from snowflake.account_usage.query_history
     where ${condition}`;
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
      const binds = [request.limit];
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
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
