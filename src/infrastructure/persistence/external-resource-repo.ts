import {
  Db,
  DeleteResult,
  Document,
  FindCursor,
  InsertManyResult,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import {
  IExternalResourceRepo,
  ExternalResourceQueryDto,
} from '../../domain/external-resource/i-external-resource-repo';
import {
  ExternalResourceType,
  ExternalResource,
  ExternalResourceProperties,
} from '../../domain/entities/external-resource';

interface ExternalResourcePersistence {
  _id: ObjectId;
  type: ExternalResourceType;
  name: string;
  lineageId: string;
  organizationId: string;
}

interface ExternalResourceQueryFilter {
  name?: RegExp;
  type?: string;
  lineageId: string;
  organizationId: string;
}

const collectionName = 'externalresource';

export default class ExternalResourceRepo implements IExternalResourceRepo {
  findOne = async (
    id: string,
    dbConnection: Db
  ): Promise<ExternalResource | null> => {
    try {
      const result: any = await dbConnection
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  findBy = async (
    externalresourceQueryDto: ExternalResourceQueryDto,
    dbConnection: Db
  ): Promise<ExternalResource[]> => {
    try {
      if (!Object.keys(externalresourceQueryDto).length)
        return await this.all(dbConnection);

      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(externalresourceQueryDto)));
      const results = await result.toArray();

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #buildFilter = (
    externalresourceQueryDto: ExternalResourceQueryDto
  ): ExternalResourceQueryFilter => {
    const filter: ExternalResourceQueryFilter = {
      lineageId: externalresourceQueryDto.lineageId,
      organizationId: externalresourceQueryDto.organizationId,
    };

    if (
      typeof externalresourceQueryDto.name === 'string' &&
      externalresourceQueryDto.name
    )
      filter.name = new RegExp(`^${externalresourceQueryDto.name}$`, 'i');
    if (externalresourceQueryDto.type)
      filter.type = externalresourceQueryDto.type;

    return filter;
  };

  all = async (dbConnection: Db): Promise<ExternalResource[]> => {
    try {
      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find();
      const results = await result.toArray();

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  insertOne = async (
    externalresource: ExternalResource,
    dbConnection: Db
  ): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(externalresource)));

      if (!result.acknowledged)
        throw new Error(
          'ExternalResource creation failed. Insert not acknowledged'
        );

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  insertMany = async (
    externalresources: ExternalResource[],
    dbConnection: Db
  ): Promise<string[]> => {
    try {
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          externalresources.map((element) =>
            this.#toPersistence(sanitize(element))
          )
        );

      if (!result.acknowledged)
        throw new Error('Logic creations failed. Inserts not acknowledged');

      return Object.keys(result.insertedIds).map((key) =>
        result.insertedIds[parseInt(key, 10)].toHexString()
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  deleteOne = async (id: string, dbConnection: Db): Promise<string> => {
    try {
      const result: DeleteResult = await dbConnection
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error(
          'ExternalResource delete failed. Delete not acknowledged'
        );

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #toEntity = (
    externalresourceProperties: ExternalResourceProperties
  ): ExternalResource => ExternalResource.create(externalresourceProperties);

  #buildProperties = (
    externalresource: ExternalResourcePersistence
  ): ExternalResourceProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: externalresource._id.toHexString(),
    type: externalresource.type,
    name: externalresource.name,
    lineageId: externalresource.lineageId,
    organizationId: externalresource.organizationId,
  });

  #toPersistence = (externalresource: ExternalResource): Document => ({
    _id: ObjectId.createFromHexString(externalresource.id),
    type: externalresource.type,
    name: externalresource.name,
    lineageId: externalresource.lineageId,
    organizationId: externalresource.organizationId,
  });
}
