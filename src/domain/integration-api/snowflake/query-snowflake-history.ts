import Result from '../../value-types/transient-types/result';
import IUseCase from '../../services/use-case';
import { BiType } from '../../value-types/bilayer';
import {
  SnowflakeQueryResultDto,
} from '../i-integration-api-repo';
import { QuerySnowflake } from './query-snowflake';

export interface QuerySfQueryHistoryRequestDto {
  biType: BiType;
  limit: number;
  targetOrganizationId?: string;
}

export interface QuerySfQueryHistoryAuthDto {
  jwt: string;
  callerOrganizationId?: string;
}

export type QuerySfQueryHistoryResponseDto = Result<SnowflakeQueryResultDto>;

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

  static #buildQuery = (limit: number, biType: BiType): string => {
    let condition = '';
    let regex = '';
    const limitNumber = limit;
    switch (biType) {
      case 'Mode': {
        // eslint-disable-next-line no-useless-escape
        regex = 'https://modeanalytics.com[^s"]+';
        condition = `REGEXP_COUNT(QUERY_TEXT,'${regex}') > 0
        limit ${limitNumber}`;
        break;
      }
      case 'Tableau': {
        // eslint-disable-next-line no-useless-escape
        regex = '"TableauSQL"';
        condition = `REGEXP_COUNT(QUERY_TEXT,'${regex}') > 0
         limit ${limitNumber}`;
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
        limit ${limitNumber}`;
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
    if (!request.targetOrganizationId && !auth.callerOrganizationId)
      throw new Error('No organization Id instance provided');
    if (request.targetOrganizationId && auth.callerOrganizationId)
      throw new Error('callerOrgId and targetOrgId provided. Not allowed');

    let organizationId: string;
    if (auth.callerOrganizationId) organizationId = auth.callerOrganizationId;
    else if (request.targetOrganizationId)
      organizationId = request.targetOrganizationId;
    else throw new Error('Unhandled organization id declaration');

   try {
      const queryResult =
        await this.#querySnowflake.execute(
          {
            query: QuerySfQueryHistory.#buildQuery(
              request.limit,
              request.biType
            ),
            targetOrganizationId: organizationId,
          },
          {jwt: auth.jwt}
        );

      if(!queryResult.success) throw new Error(queryResult.error);
      if(!queryResult.value) throw new Error('Query result is missing value');

      return Result.ok(queryResult.value);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
