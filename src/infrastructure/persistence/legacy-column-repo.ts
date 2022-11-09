import {
  AnyBulkWriteOperation,
  BulkWriteResult,
  Db,
  DeleteResult,
  Document,
  FindCursor,
  InsertManyResult,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { ColumnQueryDto, ILegacyColumnRepo } from '../../domain/column/i-column-repo';
import { Column, ColumnProperties, parseColumnDataType } from '../../domain/entities/column';

interface ColumnPersistence {
  _id: ObjectId;
  relationName: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageIds: string[];
  organizationId: string;
}

interface ColumnQueryFilter {
  relationName?: RegExp | { [key: string]: RegExp[] };
  name?: RegExp | { [key: string]: RegExp[] };
  index?: string;
  type?: string;
  materializationId?: string | { [key: string]: string[] };
  lineageIds: string;
  organizationId: string;
}

const collectionName = 'column';

export default class ColumnRepo implements ILegacyColumnRepo {
  findOne = async (id: string, dbConnection: Db): Promise<Column | null> => {
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
    columnQueryDto: ColumnQueryDto,
    dbConnection: Db
  ): Promise<Column[]> => {
    try {
      if (!Object.keys(columnQueryDto).length)
        return await this.all(dbConnection);

      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(columnQueryDto)));
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

  #buildFilter = (columnQueryDto: ColumnQueryDto): ColumnQueryFilter => {
    const filter: ColumnQueryFilter = {
      lineageIds: columnQueryDto.lineageId,
      organizationId: columnQueryDto.organizationId,
    };

    if (
      typeof columnQueryDto.relationName === 'string' &&
      columnQueryDto.relationName
    )
      filter.relationName = new RegExp(`^${columnQueryDto.relationName}$`, 'i');
    if (columnQueryDto.relationName instanceof Array)
      filter.relationName = {
        $in: columnQueryDto.relationName.map(
          (element) => new RegExp(`^${element}$`, 'i')
        ),
      };

    if (typeof columnQueryDto.name === 'string' && columnQueryDto.name)
      filter.name = new RegExp(`^${columnQueryDto.name}$`, 'i');
    if (columnQueryDto.name instanceof Array)
      filter.name = {
        $in: columnQueryDto.name.map(
          (element) => new RegExp(`^${element}$`, 'i')
        ),
      };

    if (columnQueryDto.index) filter.index = columnQueryDto.index;
    if (columnQueryDto.type) filter.type = columnQueryDto.type;

    if (
      typeof columnQueryDto.materializationId === 'string' &&
      columnQueryDto.materializationId
    )
      filter.materializationId = columnQueryDto.materializationId;
    if (columnQueryDto.materializationId instanceof Array)
      filter.materializationId = { $in: columnQueryDto.materializationId };

    return filter;
  };

  all = async (dbConnection: Db): Promise<Column[]> => {
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

  insertOne = async (column: Column, dbConnection: Db): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(column)));

      if (!result.acknowledged)
        throw new Error('Column creation failed. Insert not acknowledged');

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  insertMany = async (
    columns: Column[],
    dbConnection: Db
  ): Promise<string[]> => {
    try {
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          columns.map((element) => this.#toPersistence(sanitize(element)))
        );

      if (!result.acknowledged)
        throw new Error('Column creations failed. Inserts not acknowledged');

      return Object.keys(result.insertedIds).map((key) =>
        result.insertedIds[parseInt(key, 10)].toHexString()
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  replaceMany = async (
    columns: Column[],
    dbConnection: Db
  ): Promise<number> => {
    try {
      const operations: AnyBulkWriteOperation<Document>[] = columns.map(
        (el) => ({
          replaceOne: {
            filter: { _id: new ObjectId(sanitize(el.id)) },
            replacement: this.#toPersistence(el),
          },
        })
      );

      const result: BulkWriteResult = await dbConnection
        .collection(collectionName)
        .bulkWrite(operations);

      if (!result.isOk())
        throw new Error(
          `Bulk mat update failed. Update not ok. Error count: ${result.getWriteErrorCount()}`
        );

      return result.nMatched;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  deleteOne = async (id: string, dbConnection: Db): Promise<string> => {
    try {
      const result: DeleteResult = await dbConnection
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error('Column delete failed. Delete not acknowledged');

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #toEntity = (properties: ColumnProperties): Column =>
    Column.build(properties);

  #buildProperties = (column: ColumnPersistence): ColumnProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: column._id.toHexString(),
    relationName: column.relationName,
    name: column.name,
    index: column.index,
    dataType: parseColumnDataType(column.type),
    materializationId: column.materializationId,
    lineageIds: column.lineageIds,
    organizationId: column.organizationId,
  });

  #toPersistence = (column: Column): Document => ({
    _id: ObjectId.createFromHexString(column.id),
    relationName: column.relationName,
    name: column.name,
    index: column.index,
    type: column.dataType,
    materializationId: column.materializationId,
    lineageIds: column.lineageIds,
    organizationId: column.organizationId,
  });
}
