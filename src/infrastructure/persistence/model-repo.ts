import {
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
import { IModelRepo, ModelQueryDto } from '../../domain/model/i-model-repo';
import { Model, ModelProperties } from '../../domain/entities/model';

interface ModelPersistence {
  _id: string;
  location: string;
  sql: string;
  statementReferences: [string, string][][];
}

interface ModelQueryFilter {
  location?: string;
}

const collectionName = 'model';

export default class ModelRepo implements IModelRepo {
  findOne = async (id: string): Promise<Model | null> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: any = await db
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      close(client);

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findBy = async (modelQueryDto: ModelQueryDto): Promise<Model[]> => {
    try {
      if (!Object.keys(modelQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(modelQueryDto)));
      const results = await result.toArray();

      close(client);

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

  #buildFilter = (modelQueryDto: ModelQueryDto): ModelQueryFilter => {
    const filter: { [key: string]: any } = {};

    if (modelQueryDto.location) filter.location = modelQueryDto.location;

    return filter;
  };

  all = async (): Promise<Model[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: FindCursor = await db.collection(collectionName).find();
      const results = await result.toArray();

      close(client);

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

  insertOne = async (model: Model): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(model)));

      if (!result.acknowledged)
        throw new Error('Model creation failed. Insert not acknowledged');

      close(client);

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  public deleteOne = async (id: string): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: DeleteResult = await db
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error('Model delete failed. Delete not acknowledged');

      close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (modelProperties: ModelProperties): Model =>
    Model.create(modelProperties);

  #buildProperties = (model: ModelPersistence): ModelProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: model._id,
    location: model.location,
    sql: model.sql,
    statementReferences: model.statementReferences,
  });

  #toPersistence = (model: Model): Document => ({
    _id: ObjectId.createFromHexString(model.id),
    sql: model.sql,
    statementReferences: model.statementReferences,
  });
}
