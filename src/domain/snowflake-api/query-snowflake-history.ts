import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { BiType } from '../value-types/bilayer';
import { SnowflakeQueryResult } from './i-snowflake-api-repo';
import { QuerySnowflake } from './query-snowflake';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';

export interface QuerySfQueryHistoryRequestDto {
  biType: BiType;
  limit: number;
  profile: SnowflakeProfileDto;
  targetOrgId?: string;
}

export interface QuerySfQueryHistoryAuthDto {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export type QuerySfQueryHistoryResponseDto = Result<SnowflakeQueryResult>;

export class QuerySfQueryHistory
  implements
    IUseCase<
      QuerySfQueryHistoryRequestDto,
      QuerySfQueryHistoryResponseDto,
      QuerySfQueryHistoryAuthDto
    >
{
  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  static #buildQuery = (biType: BiType): string => {
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
    auth: QuerySfQueryHistoryAuthDto
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
          targetOrgId: request.targetOrgId,
          profile: request.profile
        },
        auth
      );

      if (!queryResult.success) throw new Error(queryResult.error);
      if (!queryResult.value) throw new Error('Query result is missing value');

      return Result.ok(queryResult.value);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
