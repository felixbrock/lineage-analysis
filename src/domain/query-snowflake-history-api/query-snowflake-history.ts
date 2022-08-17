import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IQueryHistoryApiRepo } from './i-query-history-api-repo';
import { QueryHistoryDto } from './query-history-dto';
import BiLayer from '../value-types/bilayer';

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
    
    let searchTerm = '';
    const limitNumber = request.limit;
    switch(request.biLayer) { 
      case BiLayer.mode: { 
        searchTerm = 'modeanalytics.com';
         break; 
      } 
      case BiLayer.tableau: { 
         searchTerm = 'TableauSQL';
         break; 
      } 
      default: { 
         searchTerm = ''; 
         break; 
      } 
   }; 
    
    const sqlQuery = `select QUERY_TEXT from snowflake.account_usage.query_history
     where CHARINDEX('${searchTerm}', QUERY_TEXT) > 0
     AND CHARINDEX('CHARINDEX', QUERY_TEXT) = 0 limit ${limitNumber}`;
 
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
