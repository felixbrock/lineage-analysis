import axios from 'axios';
import { IQueryHistoryApiRepo } from '../../domain/query-history-api/i-query-history-api-repo';

export default class QueryHistoryApiRepoImpl
  implements IQueryHistoryApiRepo
{


  getQueryHistory = async (
    sqlQuery: string
    // jwt: string
  ): Promise<any> => {
    try {

      const apiRoot = 'http://localhost:3002/api/v1';

      const response = await axios.post(
        `${apiRoot}/snowflake/query`,
        { query: sqlQuery },
      );
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) {
        return Promise.reject(error.message);
      }
      return Promise.reject(new Error('Unknown error occured'));
    }
  };
}
