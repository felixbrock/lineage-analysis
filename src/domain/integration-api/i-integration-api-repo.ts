
export interface SnowflakeProfileDto {
  id: string;
  accountId: string;
  username: string;
  password: string;
  organizationId: string;
  warehouseName: string;
}

export interface IIntegrationApiRepo {
  getSnowflakeProfile(jwt: string, targetOrgId?: string): Promise<SnowflakeProfileDto>;
}
