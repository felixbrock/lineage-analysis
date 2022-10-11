import axios, { AxiosRequestConfig } from 'axios';
import { appConfig } from '../../config';
import { IQueryHistoryApiRepo } from '../../domain/query-snowflake-history-api/i-query-history-api-repo';

export default class QuerySnowflakeHistoryApiRepo
  implements IQueryHistoryApiRepo
{
  getQueryHistory = async (sqlQuery: string, organizationId: string, jwt: string): Promise<any> => {
    try {
      const config: AxiosRequestConfig = {
        headers: {
          Authorization: `Bearer ${jwt}`,
        },
      };

      const response = await axios.post(
        `${appConfig.apiRoot.integrationService}/api/v1/snowflake/query`,
        { query: sqlQuery, targetOrganizationId: organizationId},
        config
      );

      const jsonResponse = response.data;
      if (response.status !== 201) throw new Error(jsonResponse.message);
      return jsonResponse;
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };
}
