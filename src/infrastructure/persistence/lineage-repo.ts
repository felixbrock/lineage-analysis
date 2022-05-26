import {
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
import { ILineageRepo } from '../../domain/lineage/i-lineage-repo';
import { Lineage, LineageProperties } from '../../domain/entities/lineage';
import { performance } from 'perf_hooks';

interface LineagePersistence {
  _id: ObjectId;
  createdAt: number;
}

const collectionName = 'lineage';

export default class LineageRepo implements ILineageRepo {
  findOne = async (id: string): Promise<Lineage | null> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: any = await db
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      await close(client);

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findCurrent = async (): Promise<Lineage | null> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: any = await db
        .collection(collectionName)
        // todo- index on createdAt
        // .find({}, {createdAt: 1, _id:0}).sort({createdAt: -1}).limit(1);
        .find()
        .sort({ createdAt: -1 })
        .limit(1);
      await close(client);

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  all = async (): Promise<Lineage[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: FindCursor = await db.collection(collectionName).find();
      const results = await result.toArray();

      await close(client);

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

  insertOne = async (lineage: Lineage): Promise<string> => {
    const start = performance.now();
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(lineage)));

      if (!result.acknowledged)
        throw new Error('Lineage creation failed. Insert not acknowledged');

      await close(client);

      const end = performance.now();
      console.log("--------------------------------------");
      console.log(`lineage insert one took ${end - start} milliseconds` );
      console.log("--------------------------------------");

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
        throw new Error('Lineage delete failed. Delete not acknowledged');

      await close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (lineageProperties: LineageProperties): Lineage =>
    Lineage.create(lineageProperties);

  #buildProperties = (lineage: LineagePersistence): LineageProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: lineage._id.toHexString(),
    createdAt: lineage.createdAt,
  });

  #toPersistence = (lineage: Lineage): Document => ({
    _id: ObjectId.createFromHexString(lineage.id),
    createdAt: lineage.createdAt,
  });
}
