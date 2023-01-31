import axios, { AxiosRequestConfig } from 'axios';
import { appConfig } from '../../config';
import {
  DeletionMode,
  IObservabilityApiRepo,
} from '../../domain/observability-api/i-observability-api-repo';

export default class ObservabilityApiRepo implements IObservabilityApiRepo {
  deleteQuantTestSuites = async (
    jwt: string,
    targetResourceIds: string[],
    mode: DeletionMode
  ): Promise<void> => {
    try {
      const config: AxiosRequestConfig = {
        headers: { Authorization: `Bearer ${jwt}` },
        params: new URLSearchParams({
          targetResourceIds: targetResourceIds.join(','),
          mode,
        }),
      };

      const response = await axios.delete(
        `${appConfig.baseUrl.observabilityService}/api/v1/test-suites`,
        config
      );
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  deleteQualTestSuites = async (
    jwt: string,
    targetResourceIds: string[],
    mode: DeletionMode
  ): Promise<void> => {
    try {
      const config: AxiosRequestConfig = {
        headers: { Authorization: `Bearer ${jwt}` },
        params: new URLSearchParams({
          targetResourceIds: targetResourceIds.join(','),
          mode,
        }),
      };

      const response = await axios.delete(
        `${appConfig.baseUrl.observabilityService}/api/v1/qual-test-suites`,
        config
      );
      const jsonResponse = response.data;
      if (response.status === 200) return jsonResponse;
      throw new Error(jsonResponse.message);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };
}
