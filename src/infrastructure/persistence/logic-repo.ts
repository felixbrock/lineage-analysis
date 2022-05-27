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

import { ILogicRepo, LogicQueryDto } from '../../domain/logic/i-logic-repo';
import {
  ColumnRef,
  Logic,
  LogicProperties,
  Refs,
  MaterializationRef,
  MaterializationDefinition,
} from '../../domain/entities/logic';

type PersistenceStatementRefs = {
  [key: string]: { [key: string]: any }[];
};

type PersistenceMaterializationDefinition = { [key: string]: string };

interface LogicPersistence {
  _id: ObjectId;
  dbtModelId: string;
  sql: string;
  dependentOn: PersistenceMaterializationDefinition[];
  parsedLogic: string;
  statementRefs: PersistenceStatementRefs;
  lineageId: string;
}

interface LogicQueryFilter {
  dbtModelId?: RegExp;
  lineageId: string;
}

const collectionName = 'logic';

export default class LogicRepo implements ILogicRepo {
  findOne = async (id: string, dbConnection: Db): Promise<Logic | null> => {
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

  findBy = async (
    logicQueryDto: LogicQueryDto,
    dbConnection: Db
  ): Promise<Logic[]> => {
    try {
      if (!Object.keys(logicQueryDto).length)
        return await this.all(dbConnection);

      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(logicQueryDto)));
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

  #buildFilter = (logicQueryDto: LogicQueryDto): LogicQueryFilter => {
    const filter: LogicQueryFilter = { lineageId: logicQueryDto.lineageId };

    if (logicQueryDto.dbtModelId)
      filter.dbtModelId = new RegExp(`^${logicQueryDto.dbtModelId}$`, 'i');

    return filter;
  };

  all = async (dbConnection: Db): Promise<Logic[]> => {
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
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertOne = async (logic: Logic, dbConnection: Db): Promise<string> => {
    try {
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(logic)));

      if (!result.acknowledged)
        throw new Error('Logic creation failed. Insert not acknowledged');

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertMany = async (logics: Logic[], dbConnection: Db): Promise<string[]> => {
    try {
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          logics.map((element) => this.#toPersistence(sanitize(element)))
        );

      if (!result.acknowledged)
        throw new Error('Logic creations failed. Inserts not acknowledged');

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
        throw new Error('Logic delete failed. Delete not acknowledged');

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (logicProperties: LogicProperties): Logic =>
    Logic.build(logicProperties);

  #buildStatementRefs = (statementRefs: PersistenceStatementRefs): Refs => {
    const materializations: MaterializationRef[] =
      statementRefs.materializations.map((materialization) => ({
        name: materialization.name,
        alias: materialization.alias,
        schemaName: materialization.schemaName,
        databaseName: materialization.databaseName,
        warehouseName: materialization.warehouseName,
        type: materialization.type,
        contexts: materialization.contexts,
      }));

    const columns: ColumnRef[] = statementRefs.columns.map((column) => ({
      name: column.name,
      alias: column.alias,
      schemaName: column.schemaName,
      databaseName: column.databaseName,
      warehouseName: column.warehouseName,
      dependencyType: column.dependencyType,
      isWildcardRef: column.isWildcardRef,
      isCompoundValueRef: column.isCompoundValueRef,
      materializationName: column.materializationName,
      context: column.context,
    }));

    const wildcards: ColumnRef[] = statementRefs.wildcards.map((wildcard) => ({
      name: wildcard.name,
      alias: wildcard.alias,
      schemaName: wildcard.schemaName,
      databaseName: wildcard.databaseName,
      warehouseName: wildcard.warehouseName,
      dependencyType: wildcard.dependencyType,
      isWildcardRef: wildcard.isWildcardRef,
      isCompoundValueRef: wildcard.isCompoundValueRef,
      materializationName: wildcard.materializationName,
      context: wildcard.context,
    }));

    return { materializations, columns, wildcards };
  };

  #buildMaterializationDefinition = (
    matCatalogElement: PersistenceMaterializationDefinition
  ): MaterializationDefinition => ({
    dbtModelId: matCatalogElement.dbtModelId,
    materializationName: matCatalogElement.materializationName,
    schemaName: matCatalogElement.schemaName,
    databaseName: matCatalogElement.databaseName,
  });

  #buildProperties = (logic: LogicPersistence): LogicProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: logic._id.toHexString(),
    dbtModelId: logic.dbtModelId,
    sql: logic.sql,
    dependentOn: logic.dependentOn.map((element) =>
      this.#buildMaterializationDefinition(element)
    ),
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
