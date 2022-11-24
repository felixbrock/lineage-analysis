import axios, { AxiosRequestConfig } from 'axios';
import { appConfig } from '../../config';
import { IIntegrationApiRepo, SnowflakeProfileDto } from '../../domain/integration-api/i-integration-api-repo';

export default class IntegrationApiRepo implements IIntegrationApiRepo {
  getSnowflakeProfile = async (
    jwt: string,
    targetOrgId?: string
  ): Promise<SnowflakeProfileDto> => {
    try {

      const config: AxiosRequestConfig = {
        headers: { Authorization: `Bearer ${jwt}` },
        params: targetOrgId ? new URLSearchParams({targetOrgId}): undefined
      };

      const response = await axios.get(
        `${appConfig.baseUrl.integrationService}/api/v1/snowflake/profile`,
        config
      );
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };
}
