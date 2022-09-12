import { QueryHistoryDto } from "./query-history-dto";

export interface IQueryHistoryApiRepo {
  getQueryHistory(sqlQuery: string, organizationId: string, jwt: string): Promise<QueryHistoryDto>;
}