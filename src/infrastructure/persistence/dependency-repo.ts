import { performance } from 'perf_hooks';
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
  lineageId: string;
}

interface DependencyQueryFilter {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
}

const collectionName = 'dependency';

export default class DependencyRepo implements IDependencyRepo {
  findOne = async (id: string, dbConnection: Db): Promise<Dependency | null> => {
    
    try {
      
      const result: any = await dbConnection
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findBy = async (
    dependencyQueryDto: DependencyQueryDto, dbConnection: Db
  ): Promise<Dependency[]> => {
    const start = performance.now();
    try {
      if (!Object.keys(dependencyQueryDto).length) return await this.all(dbConnection);

      

      
      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(dependencyQueryDto)));
      const results = await result.toArray();

      

      const end = performance.now();
      console.log("--------------------------------------");
      console.log(`dependency find by took ${end - start} milliseconds` );
      console.log(`dependency DTO:`);
      console.log(`head: ${dependencyQueryDto.headId}`);
      console.log(`tail: ${dependencyQueryDto.tailId}`);
      console.log(`lineage: ${dependencyQueryDto.lineageId}`);
      console.log(`type: ${dependencyQueryDto.type}`);
      console.log("--------------------------------------");

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #buildFilter = (
    dependencyQueryDto: DependencyQueryDto
  ): DependencyQueryFilter => {
    const filter: DependencyQueryFilter = {
      lineageId: dependencyQueryDto.lineageId,
    };

    if (dependencyQueryDto.type) filter.type = dependencyQueryDto.type;
    if (dependencyQueryDto.headId)
      filter.headId = dependencyQueryDto.headId;
    if (dependencyQueryDto.tailId)
      filter.tailId = dependencyQueryDto.tailId;

    return filter;
  };

  all = async (dbConnection: Db): Promise<Dependency[]> => {
    
    try {
      
      const result: FindCursor = await dbConnection.collection(collectionName).find();
      const results = await result.toArray();

      

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertOne = async (dependency: Dependency,dbConnection: Db): Promise<string> => {
    const start = performance.now();
    
    try {
      
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(dependency)));

      if (!result.acknowledged)
        throw new Error('Dependency creation failed. Insert not acknowledged');

      

      const end = performance.now();
      console.log("--------------------------------------");
      console.log(`dependency insert one took ${end - start} milliseconds` );
      console.log("--------------------------------------");

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertMany = async (dependencies: Dependency[], dbConnection: Db): Promise<string[]> => {

    const start = performance.now();

    
    try {
      
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          dependencies.map((element) => this.#toPersistence(sanitize(element)))
        );

      if (!result.acknowledged)
        throw new Error('Dependency creations failed. Inserts not acknowledged');

      
      const end = performance.now();

      console.log("--------------------------------------");
      console.log(`dependency insert many took ${end - start} milliseconds` );
      console.log("--------------------------------------");


      return Object.keys(result.insertedIds).map(key => result.insertedIds[parseInt(key, 10)].toHexString());
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
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
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
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
    lineageId: dependency.lineageId,
  });

  #toPersistence = (dependency: Dependency): Document => ({
    _id: ObjectId.createFromHexString(dependency.id),
    type: dependency.type,
    headId: dependency.headId,
    tailId: dependency.tailId,
    lineageId: dependency.lineageId,
  });
}
