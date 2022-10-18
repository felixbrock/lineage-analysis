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
  relationName: string;
  sql: string;
  dependentOn: {
    dbtDependencyDefinitions: PersistenceMaterializationDefinition[];
    dwDependencyDefinitions: PersistenceMaterializationDefinition[];
  };
  parsedLogic: string;
  statementRefs: PersistenceStatementRefs;
  lineageIds: string[];
  organizationId: string;
}

interface LogicQueryFilter {
  relationName?: RegExp;
  lineageIds: string;
  organizationId: string;
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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #buildFilter = (logicQueryDto: LogicQueryDto): LogicQueryFilter => {
    const filter: LogicQueryFilter = {
      lineageIds: logicQueryDto.lineageId,
      organizationId: logicQueryDto.organizationId,
    };

    if (logicQueryDto.relationName)
      filter.relationName = new RegExp(`^${logicQueryDto.relationName}$`, 'i');

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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  replaceMany = async (
    logics: Logic[],
    dbConnection: Db
  ): Promise<number> => {
    try {
      const operations: AnyBulkWriteOperation<Document>[] =
        logics.map((el) => ({
          replaceOne: {
            filter: { _id: new ObjectId(sanitize(el.id)) },
            replacement: this.#toPersistence(el),
          },
        }));

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
        throw new Error('Logic delete failed. Delete not acknowledged');

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
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
    relationName: matCatalogElement.relationName,
    materializationName: matCatalogElement.materializationName,
    schemaName: matCatalogElement.schemaName,
    databaseName: matCatalogElement.databaseName,
  });

  #buildProperties = (logic: LogicPersistence): LogicProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: logic._id.toHexString(),
    relationName: logic.relationName,
    sql: logic.sql,
    dependentOn: {
      dbtDependencyDefinitions: logic.dependentOn.dbtDependencyDefinitions.map(
        (element) => this.#buildMaterializationDefinition(element)
      ),
      dwDependencyDefinitions: logic.dependentOn.dwDependencyDefinitions.map(
        (element) => this.#buildMaterializationDefinition(element)
      ),
    },
    parsedLogic: logic.parsedLogic,
    statementRefs: this.#buildStatementRefs(logic.statementRefs),
    lineageIds: logic.lineageIds,
    organizationId: logic.organizationId,
  });

  #toPersistence = (logic: Logic): Document => ({
    _id: ObjectId.createFromHexString(logic.id),
    relationName: logic.relationName,
    sql: logic.sql,
    dependentOn: logic.dependentOn,
    parsedLogic: logic.parsedLogic,
    statementRefs: logic.statementRefs,
    lineageIds: logic.lineageIds,
    organizationId: logic.organizationId,
  });
}
