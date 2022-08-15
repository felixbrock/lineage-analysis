import axios, { AxiosRequestConfig } from 'axios';
import { IQueryHistoryApiRepo } from '../../domain/query-snowflake-history-api/i-query-history-api-repo';

export default class QuerySnowflakeHistoryApiRepo
  implements IQueryHistoryApiRepo
{
  getQueryHistory = async (sqlQuery: string, jwt: string): Promise<any> => {
    try {
      const apiRoot = 'http://localhost:3002/api/v1';

      const config: AxiosRequestConfig = {
        headers: {
          Authorization: `Bearer ${jwt}`,
        },
      };

      const response = await axios.post(
        `${apiRoot}/snowflake/query`,
        { query: sqlQuery },
        config
      );

      const jsonResponse = response.data;
      if (response.status !== 201) throw new Error(jsonResponse.message);
      return jsonResponse;
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) {
        return Promise.reject(error.message);
      }
      return Promise.reject(new Error('Unknown error occured'));
    }
  };
}
