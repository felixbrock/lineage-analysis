import {
  Db,
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { ILineageRepo } from '../../domain/lineage/i-lineage-repo';
import { Lineage, LineageProperties } from '../../domain/entities/lineage';

interface LineagePersistence {
  _id: ObjectId;
  createdAt: number;
  organizationId: string;
}


const collectionName = 'lineage';

export default class LineageRepo implements ILineageRepo {
  findOne = async (id: string, dbConnection: Db): Promise<Lineage | null> => {
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

  findCurrent = async (dbConnection: Db): Promise<Lineage | null> => {
    try {
      const result: any = await dbConnection.collection(collectionName)
        // todo- index on createdAt
        // .find({}, {createdAt: 1, _id:0}).sort({createdAt: -1}).limit(1);
        .find()
        .sort({ createdAt: -1 })
        .limit(1);

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  all = async (dbConnection: Db): Promise<Lineage[]> => {
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

  insertOne = async (lineage: Lineage, dbConnection: Db): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(lineage)));

      if (!result.acknowledged)
        throw new Error('Lineage creation failed. Insert not acknowledged');

      return result.insertedId.toHexString();
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
        throw new Error('Lineage delete failed. Delete not acknowledged');

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
    organizationId: lineage.organizationId
  });

  #toPersistence = (lineage: Lineage): Document => ({
    _id: ObjectId.createFromHexString(lineage.id),
    createdAt: lineage.createdAt,
    organizationId: lineage.organizationId
  });
}
