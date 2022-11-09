import {
  ILineageRepo,
  LineageUpdateDto,
} from '../../domain/lineage/i-lineage-repo';
import { Lineage, LineageProperties } from '../../domain/entities/lineage';

interface LineagePersistence {
  _id: ObjectId;
  createdAt: string;
  organizationId: string;
  completed: boolean;
}

interface UpdateFilter {
  $set: { [key: string]: unknown };
  $push: { [key: string]: unknown };
}

const collectionName = 'lineage';

export default class LineageRepo implements ILineageRepo {
  findOne = async (dbConnection: Db, id: string): Promise<Lineage | null> => {
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

  findLatest = async (
    dbConnection: Db,
    filter: { organizationId: string; completed?: boolean }
  ): Promise<Lineage | null> => {
    try {
      const findFilter: Filter<Document> = {
        organizationId: filter.organizationId,
      };
      if (filter.completed) findFilter.completed = filter.completed;

      const result = await dbConnection
        .collection(collectionName)
        // todo- index on createdAt
        // .find({}, {createdAt: 1, _id:0}).sort({createdAt: -1}).limit(1);
        .find(filter)
        .sort({ createdAt: -1 })
        .limit(1);
      const results: any[] = await result.toArray();

      if (results.length > 1)
        throw new Error(
          'find latest lineage - multiple lineage objects returned from persistence'
        );

      if (!results.length) return null;

      return this.#toEntity(this.#buildProperties(results[0]));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  all = async (dbConnection: Db): Promise<Lineage[]> => {
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

  insertOne = async (lineage: Lineage, dbConnection: Db): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(lineage)));

      if (!result.acknowledged)
        throw new Error('Lineage creation failed. Insert not acknowledged');

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #buildUpdateFilter = async (
    updateDto: LineageUpdateDto
  ): Promise<UpdateFilter> => {
    const setFilter: { [key: string]: unknown } = {};
    const pushFilter: { [key: string]: unknown } = {};

    if (updateDto.completed) setFilter.completed = updateDto.completed;

    return { $set: setFilter, $push: pushFilter };
  };

  updateOne = async (
    id: string,
    updateDto: LineageUpdateDto,
    dbConnection: Db
  ): Promise<string> => {
    try {
      const result: Document | UpdateResult = await dbConnection
        .collection(collectionName)
        .updateOne(
          { _id: new ObjectId(sanitize(id)) },
          await this.#buildUpdateFilter(sanitize(updateDto))
        );

      if (!result.acknowledged)
        throw new Error('Test suite update failed. Update not acknowledged');

      return result.upsertedId;
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
        throw new Error('Lineage delete failed. Delete not acknowledged');

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #toEntity = (lineageProperties: LineageProperties): Lineage =>
    Lineage.create(lineageProperties);

  #buildProperties = (lineage: LineagePersistence): LineageProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: lineage._id.toHexString(),
    createdAt: lineage.createdAt,
    organizationId: lineage.organizationId,
    completed: lineage.completed,
  });

  #toPersistence = (lineage: Lineage): LineagePersistence => ({
    _id: ObjectId.createFromHexString(lineage.id),
    createdAt: lineage.createdAt,
    organizationId: lineage.organizationId,
    completed: lineage.completed,
  });
}
