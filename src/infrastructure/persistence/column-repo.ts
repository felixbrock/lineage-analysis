import {
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
import { ColumnQueryDto, IColumnRepo } from '../../domain/column/i-column-repo';
import { Column, ColumnProperties } from '../../domain/entities/column';
import { Dependency, Direction } from '../../domain/value-types/dependency';

interface DependencyPersistence {
  type: string;
  columnId: string;
  direction: string;
}

interface ColumnPersistence {
  _id: string;
  name: string;
  tableId: string;
  dependencies: DependencyPersistence[];
}

interface DependenciesQueryFilter {
  type?: string;
  columnId?: string;
  direction?: string;
}

interface ColumnQueryFilter {
  name?: string | string[];
  tableId?: string | string[];
  dependencies?: DependenciesQueryFilter;
}

const collectionName = 'column';

export default class ColumnRepo implements IColumnRepo {
  findOne = async (id: string): Promise<Column | null> => {
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

  public findBy = async (columnQueryDto: ColumnQueryDto): Promise<Column[]> => {
    try {
      if (!Object.keys(columnQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(columnQueryDto)));
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

  #buildFilter = (columnQueryDto: ColumnQueryDto): ColumnQueryFilter => {
    const filter: { [key: string]: any } = {};

    if (typeof columnQueryDto.name === 'string' && columnQueryDto.name) filter.name = columnQueryDto.name;
    if (columnQueryDto.name instanceof Array) filter.name = {$all: columnQueryDto.name};

    if (typeof columnQueryDto.tableId === 'string' && columnQueryDto.tableId) filter.tableId = columnQueryDto.tableId;
    if (columnQueryDto.tableId instanceof Array) filter.tableId = {$all: columnQueryDto.tableId};

    if (
      !columnQueryDto.dependency ||
      !Object.keys(columnQueryDto.dependency).length
    )
      return filter;

    if (columnQueryDto.dependency.type)
      filter['dependencies.type'] = columnQueryDto.dependency.type;
    if (columnQueryDto.dependency.columnId)
      filter['dependencies.columnId'] = columnQueryDto.dependency.columnId;
    if (columnQueryDto.dependency.direction)
      filter['dependencies.direction'] = columnQueryDto.dependency.direction;

    return filter;
  };

  all = async (): Promise<Column[]> => {
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

  insertOne = async (column: Column): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(column)));

      if (!result.acknowledged)
        throw new Error('Column creation failed. Insert not acknowledged');

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
        throw new Error('Column delete failed. Delete not acknowledged');

      close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (columnProperties: ColumnProperties): Column =>
    Column.create(columnProperties);

  #buildProperties = (column: ColumnPersistence): ColumnProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: column._id,
    name: column.name,
    tableId: column.tableId,
    dependencies: column.dependencies.map((dependency) => ({
      type: dependency.type,
      columnId: dependency.columnId,
      direction: dependency.direction,
    })),
  });

  #toPersistence = (column: Column): Document => ({
    _id: ObjectId.createFromHexString(column.id),
    name: column.name,
    tableId: column.tableId,
    dependencies: column.dependencies.map((dependency) => ({
      type: dependency.type,
      columnId: dependency.columnId,
      direction: dependency.direction,
    })),
  });
}
