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
  DependencyQueryDto,
  IDependencyRepo,
} from '../../domain/dependency/i-dependency-repo';
import {
  Dependency,
  DependencyProperties,
  DependencyType,
} from '../../domain/entities/dependency';

interface DependencyPersistence {
  _id: ObjectId;
  type: DependencyType;
  headId: string;
  tailId: string;
  lineageIds: string[];
  organizationId: string;
}

interface DependencyQueryFilter {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageIds: string[];
  organizationId: string;
}

const collectionName = 'dependency';

export default class DependencyRepo implements IDependencyRepo {
  findOne = async (
    id: string,
    dbConnection: Db
  ): Promise<Dependency | null> => {
    try {
      const result: any = await dbConnection
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  findBy = async (
    dependencyQueryDto: DependencyQueryDto,
    dbConnection: Db
  ): Promise<Dependency[]> => {
    try {
      if (!Object.keys(dependencyQueryDto).length)
        return await this.all(dbConnection);

      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(dependencyQueryDto)));
      const results = await result.toArray();

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #buildFilter = (
    dependencyQueryDto: DependencyQueryDto
  ): DependencyQueryFilter => {
    const filter: DependencyQueryFilter = {
      lineageIds: dependencyQueryDto.lineageIds,
      organizationId: dependencyQueryDto.organizationId
    };

    if (dependencyQueryDto.type) filter.type = dependencyQueryDto.type;
    if (dependencyQueryDto.headId) filter.headId = dependencyQueryDto.headId;
    if (dependencyQueryDto.tailId) filter.tailId = dependencyQueryDto.tailId;

    return filter;
  };

  all = async (dbConnection: Db): Promise<Dependency[]> => {
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
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  insertOne = async (
    dependency: Dependency,
    dbConnection: Db
  ): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(dependency)));

      if (!result.acknowledged)
        throw new Error('Dependency creation failed. Insert not acknowledged');


      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  insertMany = async (
    dependencies: Dependency[],
    dbConnection: Db
  ): Promise<string[]> => {

    try {
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          dependencies.map((element) => this.#toPersistence(sanitize(element)))
        );

      if (!result.acknowledged)
        throw new Error(
          'Dependency creations failed. Inserts not acknowledged'
        );

      return Object.keys(result.insertedIds).map((key) =>
        result.insertedIds[parseInt(key, 10)].toHexString()
      );
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  deleteOne = async (id: string, dbConnection: Db): Promise<string> => {
    try {
      const result: DeleteResult = await dbConnection
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error('Dependency delete failed. Delete not acknowledged');

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #toEntity = (properties: DependencyProperties): Dependency =>
    Dependency.create(properties);

  #buildProperties = (
    dependency: DependencyPersistence
  ): DependencyProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: dependency._id.toHexString(),
    type: dependency.type,
    headId: dependency.headId,
    tailId: dependency.tailId,
    lineageIds: dependency.lineageIds,
    organizationId: dependency.organizationId
  });

  #toPersistence = (dependency: Dependency): Document => ({
    _id: ObjectId.createFromHexString(dependency.id),
    type: dependency.type,
    headId: dependency.headId,
    tailId: dependency.tailId,
    lineageIds: dependency.lineageIds,
    organizationId: dependency.organizationId
  });
}
