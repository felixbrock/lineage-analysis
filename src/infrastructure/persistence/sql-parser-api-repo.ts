import axios, { AxiosRequestConfig } from 'axios';
import { URLSearchParams } from 'url';
import { appConfig } from '../../config';
import { ISQLParserApiRepo } from '../../domain/sql-parser-api/i-sql-parser-api-repo';
import getRoot from '../shared/api-root-builder';

export default class SQLParserApiRepoImpl implements ISQLParserApiRepo {
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
      if (appConfig.express.mode === 'production') gateway = this.#prodGateway;

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

      /* todo - currently consuming an object that is presenting the parse object in an alphabetically ordered structure 
      (due to the issue highlighted in sql parse service). 
      This needs to be fixed before improving the lineage service */
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      /* error code 500 is returned when we encounter a parse
           error. Returning empty fle allows us to continue and 
           create as much lineage as possible instead of failing */
      if (error instanceof Error && error.message.includes('500'))
        return { file: [{}, {}] };
      return Promise.reject(new Error(''));
    }
  };
}
