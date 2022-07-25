import { QueryHistoryDto } from "./query-history-dto";

export interface IQueryHistoryApiRepo {
  getQueryHistory(sqlQuery: string): Promise<QueryHistoryDto>;
}