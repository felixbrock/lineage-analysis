import axios, { AxiosRequestConfig } from 'axios';
import { appConfig } from '../../config';
import { IIntegrationApiRepo } from '../../domain/integration-api/i-integration-api-repo';

export default class IntegrationApiRepo implements IIntegrationApiRepo {
  #version = 'v1';

  #baseUrl = appConfig.baseUrl.integrationService;

  #apiRoot = appConfig.express.apiRoot;

  readSnowflakeProfile = async (
    targetOrgId: string,
    jwt: string
  ): Promise<SnowflakeProfileDto> => {
    try {

      const config: AxiosRequestConfig = {
        headers: { Authorization: `Bearer ${jwt}` },
      };

      const response = await axios.get(
        `${this.#baseUrl}/${this.#apiRoot}/${this.#version}/snowflake/profiles/${targetOrgId}`,
        config
      );
      const jsonResponse = response.data;
      if (response.status === 201) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };
}
