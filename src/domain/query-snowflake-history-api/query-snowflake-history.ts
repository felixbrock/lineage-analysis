import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IQueryHistoryApiRepo } from './i-query-history-api-repo';
import { QueryHistoryDto } from './query-history-dto';
import { BiLayer } from '../value-types/bilayer';

export interface QueryHistoryRequestDto {
  biLayer: BiLayer;
  limit: number;
}

export interface QuerySnowflakeHistoryAuthDto {
  jwt: string;
}

export type QueryHistoryResponseDto = Result<QueryHistoryDto>;

export class QuerySnowflakeHistory
  implements IUseCase<QueryHistoryRequestDto, QueryHistoryResponseDto, QuerySnowflakeHistoryAuthDto>
{
  readonly #queryHistoryApiRepo: IQueryHistoryApiRepo;

  constructor(queryHistoryApiRepo: IQueryHistoryApiRepo) {
    this.#queryHistoryApiRepo = queryHistoryApiRepo;
  }

  async execute(
    request: QueryHistoryRequestDto,
    auth: QuerySnowflakeHistoryAuthDto
  ): Promise<QueryHistoryResponseDto> {
    
    let condition = '';
    let regex = '';
    const limitNumber = request.limit;
    switch(request.biLayer) { 
      case 'Mode': {
        // eslint-disable-next-line no-useless-escape
        regex = 'https:\/\/modeanalytics\.com[^\s\"]+';
        condition = 
        `REGEXP_COUNT(QUERY_TEXT,'${regex}') > 0
        limit ${limitNumber}`;
        break; 
      } 
      case 'Tableau': {
        // eslint-disable-next-line no-useless-escape
         regex = '\"TableauSQL\"';
         condition = 
         `REGEXP_COUNT(QUERY_TEXT,'${regex}') > 0
         limit ${limitNumber}`;
         break; 
      }
      case 'Metabase': {
        // eslint-disable-next-line no-useless-escape
        regex = '("[A-Za-z0-9_$]+"\.){2}("[A-Za-z0-9_$]+")';
        condition = 
        `REGEXP_COUNT(QUERY_TEXT,'FROM ${regex}') > 0
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
   }; 
    
    const sqlQuery = 
    `select QUERY_TEXT from snowflake.account_usage.query_history
     where ${condition}`;
 
    try {
      const parseSQLResponse: QueryHistoryDto =
        await this.#queryHistoryApiRepo.getQueryHistory(
          sqlQuery,
          auth.jwt
        );

      if (!parseSQLResponse) throw new Error(`SQL parsing failed`);

      return Result.ok(parseSQLResponse);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
