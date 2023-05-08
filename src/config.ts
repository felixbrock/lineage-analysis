import dotenv from 'dotenv';

import path from 'path';

const dotenvConfig =
  process.env.NODE_ENV === 'development'
    ? { path: path.resolve(process.cwd(), `.env.${process.env.NODE_ENV}`) }
    : {};
dotenv.config(dotenvConfig);

const nodeEnv = process.env.NODE_ENV || 'development';
const defaultPort = 8081;
const port = process.env.PORT ? parseInt(process.env.PORT, 10) : defaultPort;
const apiRoot = process.env.API_ROOT || 'api';

const getCognitoUserPoolId = (): string => {
  switch (nodeEnv) {
    case 'development':
      return 'eu-central-1_0Z8JhFj8z';
    case 'test':
      return 'eu-central-1_0muGtKMk3';
    case 'production':
      return 'eu-central-1_0muGtKMk3';
    default:
      throw new Error('No valid nodenv provided');
  }
};

export interface BaseUrlConfig {
  sqlParser: string;
  integrationService: string;
  accountService: string;
  observabilityService: string;
}

const getBaseUrlConfig = (): BaseUrlConfig => {
  const sqlParser = process.env.BASE_URL_SQL_PARSER;
  const integrationService = process.env.BASE_URL_INTEGRATION_SERVICE;
  const accountService = process.env.BASE_URL_ACCOUNT_SERVICE;
  const observabilityService = process.env.BASE_URL_OBSERVABILITY;

  if (
    !sqlParser ||
    !integrationService ||
    !accountService ||
    !observabilityService
  )
    throw new Error('Missing Base url env values');

  return {
    sqlParser,
    integrationService,
    accountService,
    observabilityService,
  };
};

export interface MongoDbConfig {
  url: string;
  dbName: string;
}

const getMongodbConfig = (): MongoDbConfig => {
  const url = process.env.DATABASE_URL || '';
  const dbName = process.env.DATABASE_NAME || '';

  if (!url || !dbName) throw new Error('Missing Mongo DB env values');

  return { url, dbName };
};

export const appConfig = {
  express: {
    port,
    mode: nodeEnv,
    apiRoot,
  },
  cloud: {
    // serviceDiscoveryNamespace: getServiceDiscoveryNamespace(),
    userPoolId: getCognitoUserPoolId(),
    region: 'eu-central-1',
  },
  snowflake: {
    applicationName: process.env.SNOWFLAKE_APPLICATION_NAME || 'Cito_CitoApp',
    schemaName: 'lineage',
    databaseName: 'cito',
  },
  baseUrl: getBaseUrlConfig(),
  mongodb: getMongodbConfig(),
};
