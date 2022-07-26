import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IQueryHistoryApiRepo } from './i-query-history-api-repo';
import { QueryHistoryDto } from './query-history-dto';

export interface QueryHistoryRequestDto {
  biLayer: string;
  limit: number;
}

export interface QueryHistroyAuthDto {
  jwt: string;
}

export type QueryHistoryResponseDto = Result<QueryHistoryDto>;

export class QueryHistory
  implements IUseCase<QueryHistoryRequestDto, QueryHistoryResponseDto, QueryHistroyAuthDto>
{
  readonly #queryHistoryApiRepo: IQueryHistoryApiRepo;

  constructor(queryHistoryApiRepo: IQueryHistoryApiRepo) {
    this.#queryHistoryApiRepo = queryHistoryApiRepo;
  }

  async execute(
    request: QueryHistoryRequestDto,
    auth: QueryHistroyAuthDto
  ): Promise<QueryHistoryResponseDto> {
    console.log(auth);

    // let searchParam;
    // switch(request.biLayer) { 
    //     case 'mode': { 
    //         searchParam = '%modeanalytics%'
    //        break; 
    //     }
    //  };
    
    // const limit = request.limit;
    const sqlQuery = `select QUERY_TEXT from snowflake.account_usage.query_history
     where CHARINDEX('modeanalytics.com', QUERY_TEXT) > 0 limit 10`;
 
    try {
      const parseSQLResponse: QueryHistoryDto =
        await this.#queryHistoryApiRepo.getQueryHistory(
          sqlQuery
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
