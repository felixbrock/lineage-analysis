import axios, { AxiosRequestConfig } from 'axios';
import { URLSearchParams } from 'url';
import { appConfig } from '../../config';
import { ISQLParserApiRepo } from '../../domain/sql-parser-api/i-sql-parser-api-repo';
import getRoot from '../shared/api-root-builder';

export default class SQLParserApiRepoImpl
  implements ISQLParserApiRepo
{
  #path = 'sql';

  #port = '3037';

  #prodGateway = 'rteezzzwn6.execute-api.eu-central-1.amazonaws.com/Prod';

  parseOne = async (
    params: URLSearchParams,
    base64SQL: string
    // jwt: string
  ): Promise<any> => {
    try {
      let gateway = this.#port;
      if(appConfig.express.mode === 'production') gateway = this.#prodGateway;

      const apiRoot = await getRoot(gateway, this.#path, false);

      const config: AxiosRequestConfig = {
        // headers: { Authorization: `Bearer ${jwt}` },
        params,
      };

      const response = await axios.post(
        `${apiRoot}`,
        { sql: base64SQL },
        config
      );
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) {

        /* error code 500 is returned when we encounter a parse
           error. Returning empty fle allows us to continue and 
           create as much lineage as possible instead of failing */  
        if(error.message.includes('500')) return {file:[{}, {}]};
        return Promise.reject(error.message);
      }
      return Promise.reject(new Error('Unknown error occured'));
    }
  };
}
