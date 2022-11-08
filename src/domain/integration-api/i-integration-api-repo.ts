interface QueryResultElement {
  [key: string]: unknown;
}

export interface SnowflakeQueryResultDto {
  [key: string]: QueryResultElement[];
}

export interface AlertMessageConfig {
  anomalyMessagePart: string;
  occuredOn: string;
  alertId: string;
  testType: string;
  summaryPart: string;
  expectedRangePart: string;
  detectedValuePart: string;
}
export interface IIntegrationApiRepo {
  querySnowflake(body: {query: string, targetOrganizationId?: string}, jwt: string): Promise<SnowflakeQueryResultDto>;
}
