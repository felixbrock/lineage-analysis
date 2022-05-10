import {
  DeleteResult,
  Document,
  FindCursor,
  InsertManyResult,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
import { ILogicRepo, LogicQueryDto } from '../../domain/logic/i-logic-repo';
import {
  ColumnRef,
  Logic,
  LogicProperties,
  Refs,
  MaterializationRef,
} from '../../domain/entities/logic';

type PersistenceStatementRefs = {
  [key: string]: { [key: string]: any }[];
}[];

interface LogicPersistence {
  _id: ObjectId;
  dbtModelId: string;
  sql: string;
  parsedLogic: string;
  statementRefs: PersistenceStatementRefs;
  lineageId: string;
}

interface LogicQueryFilter {
  dbtModelId?: string;
  lineageId: string;
}

const collectionName = 'logic';

export default class LogicRepo implements ILogicRepo {
  findOne = async (id: string): Promise<Logic | null> => {
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

  findBy = async (logicQueryDto: LogicQueryDto): Promise<Logic[]> => {
    try {
      if (!Object.keys(logicQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(logicQueryDto)));
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

  #buildFilter = (logicQueryDto: LogicQueryDto): LogicQueryFilter => {
    const filter: LogicQueryFilter = { lineageId: logicQueryDto.lineageId };

    if (logicQueryDto.dbtModelId) filter.dbtModelId = logicQueryDto.dbtModelId;

    return filter;
  };

  all = async (): Promise<Logic[]> => {
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

  insertOne = async (logic: Logic): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(logic)));

      if (!result.acknowledged)
        throw new Error('Logic creation failed. Insert not acknowledged');

      close(client);

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertMany = async (logics: Logic[]): Promise<string[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertManyResult<Document> = await db
        .collection(collectionName)
        .insertMany(
          logics.map((element) => this.#toPersistence(sanitize(element)))
        );

      if (!result.acknowledged)
        throw new Error('Logic creations failed. Inserts not acknowledged');

      close(client);

      return Object.keys(result.insertedIds).map(key => result.insertedIds[parseInt(key, 10)].toHexString());
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
        throw new Error('Logic delete failed. Delete not acknowledged');

      close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (logicProperties: LogicProperties): Logic =>
    Logic.build(logicProperties);

  #buildStatementRefs = (statementRefs: PersistenceStatementRefs): Refs[] =>
    statementRefs.map((ref) => {
      const materializations: MaterializationRef[] = ref.materializations.map(
        (materialization) => ({
          paths: materialization.paths,
          name: materialization.name,
          alias: materialization.alias,
          schemaName: materialization.schemaName,
          databaseName: materialization.databaseName,
          warehouseName: materialization.warehouseName,
          isSelfRef: materialization.isSelfRef,
        })
      );

      const columns: ColumnRef[] = ref.columns.map((column) => ({
        path: column.path,
        name: column.name,
        alias: column.alias,
        schemaName: column.schemaName,
        databaseName: column.databaseName,
        warehouseName: column.warehouseName,
        dependencyType: column.dependencyType,
        isWildcardRef: column.isWildcardRef,
        materializationName: column.materializationName,
      }));

      const wildcards: ColumnRef[] = ref.wildcards.map((wildcard) => ({
        path: wildcard.path,
        name: wildcard.name,
        alias: wildcard.alias,
        schemaName: wildcard.schemaName,
        databaseName: wildcard.databaseName,
        warehouseName: wildcard.warehouseName,
        dependencyType: wildcard.dependencyType,
        isWildcardRef: wildcard.isWildcardRef,
        materializationName: wildcard.materializationName,
      }));

      return { materializations, columns, wildcards };
    });

  #buildProperties = (logic: LogicPersistence): LogicProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: logic._id.toHexString(),
    dbtModelId: logic.dbtModelId,
    sql: logic.sql,
    parsedLogic: logic.parsedLogic,
    statementRefs: this.#buildStatementRefs(logic.statementRefs),
    lineageId: logic.lineageId,
  });

  #toPersistence = (logic: Logic): Document => ({
    _id: ObjectId.createFromHexString(logic.id),
    dbtModelId: logic.dbtModelId,
    sql: logic.sql,
    parsedLogic: logic.parsedLogic,
    statementRefs: logic.statementRefs,
    lineageId: logic.lineageId,
  });
}
