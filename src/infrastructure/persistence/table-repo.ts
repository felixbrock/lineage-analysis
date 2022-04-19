import {
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
import { ITableRepo, TableQueryDto } from '../../domain/table/i-table-repo';
import { Table, TableProperties } from '../../domain/entities/table';

interface TablePersistence {
  _id: ObjectId;
  dbtModelId: string;
  name: string;
  modelId: string;
  lineageId: string;
}

interface TableQueryFilter {
  dbtModelId?: string;
  name?: string | { [key: string]: string[] };
  modelId?: string;
  lineageId: string;
}

const collectionName = 'table';

export default class TableRepo implements ITableRepo {
  findOne = async (id: string): Promise<Table | null> => {
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

  findBy = async (tableQueryDto: TableQueryDto): Promise<Table[]> => {
    try {
      if (!Object.keys(tableQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(tableQueryDto)));
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

  #buildFilter = (tableQueryDto: TableQueryDto): TableQueryFilter => {
    const filter: TableQueryFilter = { lineageId: tableQueryDto.lineageId };

    if (tableQueryDto.dbtModelId) filter.dbtModelId = tableQueryDto.dbtModelId;
    if (typeof tableQueryDto.name === 'string' && tableQueryDto.name)
      filter.name = tableQueryDto.name;
    if (tableQueryDto.name instanceof Array)
      filter.name = { $in: tableQueryDto.name };

    if (tableQueryDto.modelId) filter.modelId = tableQueryDto.modelId;

    return filter;
  };

  all = async (): Promise<Table[]> => {
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

  insertOne = async (table: Table): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(table)));

      if (!result.acknowledged)
        throw new Error('Table creation failed. Insert not acknowledged');

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
        throw new Error('Table delete failed. Delete not acknowledged');

      close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (tableProperties: TableProperties): Table =>
    Table.create(tableProperties);

  #buildProperties = (table: TablePersistence): TableProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: table._id.toHexString(),
    dbtModelId: table.dbtModelId,
    name: table.name,
    modelId: table.modelId,
    lineageId: table.lineageId,
  });

  #toPersistence = (table: Table): Document => ({
    _id: ObjectId.createFromHexString(table.id),
    dbtModelId: table.dbtModelId,
    name: table.name,
    modelId: table.modelId,
    lineageId: table.lineageId,
  });
}
