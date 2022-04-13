import {
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
import {
  DependencyQueryDto,
  IDependencyRepo,
} from '../../domain/dependency/i-dependency-repo';
import {
  Dependency,
  DependencyProperties,
} from '../../domain/entities/dependency';

interface DependencyPersistence {
  _id: ObjectId;
  type: string;
  headColumnId: string;
  tailColumnId: string;
  lineageId: string;
}

interface DependencyQueryFilter {
  type?: string;
  headColumnId?: string;
  tailColumnId?: string;
  lineageId: string;
}

const collectionName = 'dependency';

export default class DependencyRepo implements IDependencyRepo {
  findOne = async (id: string): Promise<Dependency | null> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: any = await db
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      close(client);

      if (!result) return null;

      return this.#toEntity(this.#buildPrototype(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findBy = async (
    dependencyQueryDto: DependencyQueryDto
  ): Promise<Dependency[]> => {
    try {
      if (!Object.keys(dependencyQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(dependencyQueryDto)));
      const results = await result.toArray();

      close(client);

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildPrototype(element))
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
    if (dependencyQueryDto.headColumnId)
      filter.headColumnId = dependencyQueryDto.headColumnId;
    if (dependencyQueryDto.tailColumnId)
      filter.tailColumnId = dependencyQueryDto.tailColumnId;

    return filter;
  };

  all = async (): Promise<Dependency[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: FindCursor = await db.collection(collectionName).find();
      const results = await result.toArray();

      close(client);

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildPrototype(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertOne = async (dependency: Dependency): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(dependency)));

      if (!result.acknowledged)
        throw new Error('Dependency creation failed. Insert not acknowledged');

      close(client);

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  deleteOne = async (id: string): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: DeleteResult = await db
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error('Dependency delete failed. Delete not acknowledged');

      close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (properties: DependencyProperties): Dependency =>
    Dependency.create(properties);

  #buildPrototype = (
    dependency: DependencyPersistence
  ): DependencyProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: dependency._id.toHexString(),
    type: dependency.type,
    headColumnId: dependency.headColumnId,
    tailColumnId: dependency.tailColumnId,
    lineageId: dependency.lineageId,
  });

  #toPersistence = (dependency: Dependency): Document => ({
    _id: ObjectId.createFromHexString(dependency.id),
    type: dependency.type,
    headColumnId: dependency.headColumnId,
    tailColumnId: dependency.tailColumnId,
    lineageId: dependency.lineageId,
  });
}
