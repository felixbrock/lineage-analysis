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

import { ColumnQueryDto, IColumnRepo } from '../../domain/column/i-column-repo';
import { Column, ColumnProperties } from '../../domain/entities/column';

interface ColumnPersistence {
  _id: ObjectId;
  dbtModelId: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
  organizationId: string;
}

interface ColumnQueryFilter {
  dbtModelId?: RegExp | { [key: string]: RegExp[] };
  name?: RegExp | { [key: string]: RegExp[] };
  index?: string;
  type?: string;
  materializationId?: string | { [key: string]: string[] };
  lineageId: string;
  organizationId: string;
}

const collectionName = 'column';

export default class ColumnRepo implements IColumnRepo {
  findOne = async (id: string, dbConnection: Db): Promise<Column | null> => {
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

  findBy = async (columnQueryDto: ColumnQueryDto, dbConnection: Db): Promise<Column[]> => {
    try {
      if (!Object.keys(columnQueryDto).length) return await this.all(dbConnection);   

      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(columnQueryDto)));
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

  #buildFilter = (columnQueryDto: ColumnQueryDto): ColumnQueryFilter => {
    const filter: ColumnQueryFilter = { lineageId: columnQueryDto.lineageId, organizationId: columnQueryDto.organizationId };

    if (
      typeof columnQueryDto.dbtModelId === 'string' &&
      columnQueryDto.dbtModelId
    )
      filter.dbtModelId = new RegExp(`^${columnQueryDto.dbtModelId}$`, 'i');
    if (columnQueryDto.dbtModelId instanceof Array)
      filter.dbtModelId = {
        $in: columnQueryDto.dbtModelId.map(
          (element) => new RegExp(`^${element}$`, 'i')
        ),
      };

    if (typeof columnQueryDto.name === 'string' && columnQueryDto.name)
      filter.name = new RegExp(`^${columnQueryDto.name}$`, 'i');
    if (columnQueryDto.name instanceof Array)
      filter.name = {
        $in: columnQueryDto.name.map((element) => new RegExp(`^${element}$`, 'i')),
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

  insertOne = async (column: Column, dbConnection: Db): Promise<string> => {
    
    try {
      
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(column)));

      if (!result.acknowledged)
        throw new Error('Column creation failed. Insert not acknowledged');

      

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertMany = async (columns: Column[], dbConnection: Db): Promise<string[]> => {
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
        throw new Error('Column delete failed. Delete not acknowledged');

      

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (properties: ColumnProperties): Column =>
    Column.create(properties);

  #buildProperties = (column: ColumnPersistence): ColumnProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: column._id.toHexString(),
    dbtModelId: column.dbtModelId,
    name: column.name,
    index: column.index,
    type: column.type,
    materializationId: column.materializationId,
    lineageId: column.lineageId,
    organizationId: column.organizationId
  });

  #toPersistence = (column: Column): Document => ({
    _id: ObjectId.createFromHexString(column.id),
    dbtModelId: column.dbtModelId,
    name: column.name,
    index: column.index,
    type: column.type,
    materializationId: column.materializationId,
    lineageId: column.lineageId,
    organizationId: column.organizationId
  });
}
