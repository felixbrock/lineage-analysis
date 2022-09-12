import axios, { AxiosRequestConfig } from 'axios';
import { appConfig } from '../../config';
import { IQueryHistoryApiRepo } from '../../domain/query-snowflake-history-api/i-query-history-api-repo';
import getRoot from '../shared/api-root-builder';

export default class QuerySnowflakeHistoryApiRepo
  implements IQueryHistoryApiRepo
{
  #path = 'api/v1';

  #port = '3002';

  #prodGateway = 'wej7xjkvug.execute-api.eu-central-1.amazonaws.com/production';

  getQueryHistory = async (sqlQuery: string, organizationId: string, jwt: string): Promise<any> => {
    try {
      let gateway = this.#port;
      if(appConfig.express.mode === 'production') gateway = this.#prodGateway;

      const apiRoot = await getRoot(gateway, this.#path, false);

      const config: AxiosRequestConfig = {
        headers: {
          Authorization: `Bearer ${jwt}`,
        },
      };

      const response = await axios.post(
        `${apiRoot}/snowflake/query`,
        { query: sqlQuery, targetOrganizationId: organizationId},
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
