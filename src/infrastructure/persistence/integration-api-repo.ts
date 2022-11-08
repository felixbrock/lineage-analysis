import axios, { AxiosRequestConfig } from 'axios';
import {
  IIntegrationApiRepo,
  QuerySfQueryHistoryDto,
  SnowflakeQueryResultDto,
} from '../../domain/integration-api/i-integration-api-repo';
import { appConfig } from '../../config';

export default class IntegrationApiRepo implements IIntegrationApiRepo {
  #version = 'v1';

  #baseUrl = appConfig.baseUrl.integrationService;

  #apiRoot = appConfig.express.apiRoot;

  querySnowflake = async (
    body: {query: string, targetOrganizationId?: string},
    jwt: string
  ): Promise<SnowflakeQueryResultDto> => {
    try {

      const config: AxiosRequestConfig = {
        headers: { Authorization: `Bearer ${jwt}` },
      };

      const response = await axios.post(
        `${this.#baseUrl}/${this.#apiRoot}/${this.#version}/snowflake/query`,
        body,
        config
      );
      const jsonResponse = response.data;
      if (response.status === 201) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };
}
