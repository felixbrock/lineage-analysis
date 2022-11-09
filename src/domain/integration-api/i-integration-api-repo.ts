interface QueryResultElement {
  [key: string]: unknown;
}

export type OrganizationLevelQueryResult = QueryResultElement[];

export interface SnowflakeQueryResult {
  [key: string]: OrganizationLevelQueryResult;
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
  querySnowflake(body: {query: string, targetOrganizationId?: string}, jwt: string): Promise<SnowflakeQueryResult>;
}
