import axios, { AxiosRequestConfig } from 'axios';
import { URLSearchParams } from 'url';
import { appConfig } from '../../config';
import { AccountDto } from '../../domain/account-api/account-dto';
import { IAccountApiRepo } from '../../domain/account-api/i-account-api-repo';

export default class AccountApiRepo implements IAccountApiRepo {
 
  getBy = async (
    params: URLSearchParams,
    jwt: string
  ): Promise<AccountDto[]> => {
    try {

      const config: AxiosRequestConfig = {
        headers: { Authorization: `Bearer ${jwt}` },
        params,
      };

      const response = await axios.get(`${appConfig.baseUrl.accountService}/api/v1/accounts`, config);
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if(error instanceof Error ) console.error(error.stack); else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };
}
