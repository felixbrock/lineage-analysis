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

// const getServiceDiscoveryNamespace = (): string | null => {
//   switch (nodeEnv) {
//     case 'development':
//       return null;
//     case 'test':
//       return 'lineage-staging';
//     case 'production':
//       return 'lineage';
//     default:
//       throw new Error('No valid nodenv value provided');
//   }
// };

export interface MongoDbConfig {
  url: string;
  dbName: string;
}

const getMongodbConfig = (): MongoDbConfig => ({
  url: process.env.DATABASE_URL || '',
  dbName: process.env.DATABASE_NAME || '',
});

const getCognitoUserPoolId = (): string => {
  switch (nodeEnv) {
    case 'development':
      return 'eu-central-1_0Z8JhFj8z';
    case 'test':
      return '';
    case 'production':
      return 'eu-central-1_0muGtKMk3';
    default:
      throw new Error('No valid nodenv provided');
  }
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
  apiRoot: {
    sqlParser: process.env.API_ROOT_SQL_PARSER,
    integrationService: process.env.API_ROOT_INTEGRATION_SERVICE,
    accountService: process.env.API_ROOT_ACCOUNT_SERVICE
  },
  mongodb: getMongodbConfig(),
};
