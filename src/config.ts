import dotenv from 'dotenv';

dotenv.config();

const nodeEnv = process.env.NODE_ENV || 'development';
const defaultPort = 8081;
const port = process.env.PORT ? parseInt(process.env.PORT, 10) : defaultPort;
const apiRoot = process.env.API_ROOT || 'api';

const getServiceDiscoveryNamespace = (): string | null => {
  switch (nodeEnv) {
    case 'development':
      return null;
    case 'test':
      return 'lineage-staging';
    case 'production':
      return 'lineage';
    default:
      throw new Error('No valid nodenv value provided');
  }
};

export interface MongoDbConfig {
  url: string;
  dbName: string;
}

const getMongodbConfig = (): MongoDbConfig => {
  switch (nodeEnv) {
    case 'development':
      return {
        url: process.env.DATABASE_DEV_URL || '',
        dbName: process.env.DATABASE_DEV_NAME || '',
      };
    case 'test':
      return {
        url: process.env.DATABASE_TEST_URL || '',
        dbName: process.env.DATABASE_TEST_NAME || '',
      };
    case 'production':
      return {
        url: process.env.DATABASE_URL_PROD || '',
        dbName: process.env.DATABASE_NAME_PROD || '',
      };
    default:
      throw new Error('Node environment mismatch');
  }
};

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
    serviceDiscoveryNamespace: getServiceDiscoveryNamespace(),
    userPoolId: getCognitoUserPoolId(),
    region: 'eu-central-1',
  },
  mongodb: {
    ...getMongodbConfig(),
  },
};
