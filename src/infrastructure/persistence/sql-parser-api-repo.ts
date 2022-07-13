import axios, { AxiosRequestConfig } from 'axios';
import { URLSearchParams } from 'url';
import { ISQLParserApiRepo } from '../../domain/sql-parser-api/i-sql-parser-api-repo';

export default class SQLParserApiRepoImpl
  implements ISQLParserApiRepo
{
  // #path = 'api/v1';

  // #serviceName = 'account';

  // #port = '8081';

  parseOne = async (
    params: URLSearchParams,
    base64SQL: string
    // jwt: string
  ): Promise<any> => {
    try {
      // const apiRoot = await getRoot(this.#serviceName, this.#port, this.#path);
      const apiRoot = 'http://127.0.0.1:5000/';

      const config: AxiosRequestConfig = {
        // headers: { Authorization: `Bearer ${jwt}` },
        params,
      };

      const response = await axios.post(
        `${apiRoot}/sql`,
        { sql: base64SQL },
        config
      );
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) {
        if(error.message.includes('500')) return {file:[{}, {}]};
        return Promise.reject(error.message);
      }
      return Promise.reject(new Error('Unknown error occured'));
    }
  };
}
