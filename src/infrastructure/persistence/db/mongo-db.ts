import { Db, MongoClient, ServerApiVersion } from 'mongodb';
import { appConfig } from '../../../config';

export const createClient = (): MongoClient => new MongoClient(appConfig.mongodb.url, { serverApi: ServerApiVersion.v1 });

export const connect = async (client: MongoClient): Promise<Db> => {
  await client.connect();
  return client.db(appConfig.mongodb.dbName);
};

export const close = async (client: MongoClient): Promise<void> => client.close();
