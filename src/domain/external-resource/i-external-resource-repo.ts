import {
  ExternalResource,
  ExternalResourceType,
} from '../entities/external-resource';
import { DbConnection } from '../services/i-db';

export interface ExternalResourceQueryDto {
  name?: string;
  type?: ExternalResourceType;
  lineageId: string;
  organizationId: string;
}

export interface IExternalResourceRepo {
  findOne(
    id: string,
    dbConnection: DbConnection
  ): Promise<ExternalResource | null>;
  findBy(
    ExternalResourceQueryDto: ExternalResourceQueryDto,
    dbConnection: DbConnection
  ): Promise<ExternalResource[]>;
  all(dbConnection: DbConnection): Promise<ExternalResource[]>;
  insertOne(
    ExternalResource: ExternalResource,
    dbConnection: DbConnection
  ): Promise<string>;
  insertMany(
    externalresources: ExternalResource[],
    dbConnection: DbConnection
  ): Promise<string[]>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
