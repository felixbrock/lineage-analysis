import {  Auth, ILogicRepo, LogicQueryDto } from '../../domain/logic/i-logic-repo';
import {
  ColumnRef,
  Logic,
  LogicProps,
  Refs,
  MaterializationRef,
  MaterializationDefinition,
} from '../../domain/entities/logic';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import { ColumnDefinition } from './shared/query';

type PersistenceStatementRefs = {
  [key: string]: { [key: string]: any }[];
};

type PersistenceMaterializationDefinition = { [key: string]: string };

export default class LogicRepo implements ILogicRepo {
    readonly #matName = 'logic';
  
    readonly #querySnowflake: QuerySnowflake;
  
    constructor(querySnowflake: QuerySnowflake) {
      this.#querySnowflake = querySnowflake;
    }
  
    findOne = async (
      logicId: string,
      targetOrgId: string,
      auth: Auth
    ): Promise<Logic | null> => {
      try {
        const queryText = `select * from cito.lineage.${this.#matName}
        } where id = ?;`;
  
        // using binds to tell snowflake to escape params to avoid sql injection attack
        const binds: (string | number)[] = [logicId];
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
        if (result.value.length !== 1)
          throw new Error(`Multiple or no logic entities with id found`);
  
        const {
          ID: id,
          RELATION_NAME: relationName,
          SQL: sql,
          DEPENDENT_ON: dependentOn,
          PARSED_LOGIC: parsedLogic,
          STATEMENT_REFS: statementRefs,
          LINEAGE_IDS: lineageIds,
        } = result.value[0];
  
        if (
          typeof id !== 'string' ||
          typeof relationName !== 'string' ||
          typeof sql !== 'string' ||
          typeof dependentOn !== 'string' ||
          typeof parsedLogic !== 'string' ||
          typeof statementRefs !== 'string' ||
          typeof lineageIds !== 'string' 
        )
          throw new Error(
            'Retrieved unexpected logic field types from persistence'
          );
  
        return this.#toEntity({ id, sql, relationName, dependentOn: JSON.parse(dependentOn), lineageIds: JSON.parse(lineageIds), parsedLogic: JSON.parse(parsedLogic), statementRefs: JSON.parse(statementRefs)  });
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error(''));
      }
    };
  
    findBy = async (
      logicQueryDto: LogicQueryDto,
      targetOrgId: string,
      auth: Auth
    ): Promise<Logic[]> => {
      try {
        if (!Object.keys(logicQueryDto).length)
        return await this.all(targetOrgId, auth);

        // using binds to tell snowflake to escape params to avoid sql injection attack
        const binds: (string | number)[] = [logicQueryDto.logicId];
        if(logicQueryDto.relationName) binds.push(logicQueryDto.relationName);

        const queryText = `select * from cito.lineage.${this.#matName}
      } where id = ? ${logicQueryDto.relationName? 'and relation_name = ?' : ''};`;


      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no logic entities with id found`);

      

        return result.value.map((el) => {
          const {
            ID: id,
            RELATION_NAME: relationName,
            SQL: sql,
            DEPENDENT_ON: dependentOn,
            PARSED_LOGIC: parsedLogic,
            STATEMENT_REFS: statementRefs,
            LINEAGE_IDS: lineageIds,
          } = el;
    
          if (
            typeof id !== 'string' ||
            typeof relationName !== 'string' ||
            typeof sql !== 'string' ||
            typeof dependentOn !== 'string' ||
            typeof parsedLogic !== 'string' ||
            typeof statementRefs !== 'string' ||
            typeof lineageIds !== 'string' 
          )
            throw new Error(
              'Retrieved unexpected logic field types from persistence'
            );
  
          return this.#toEntity({ id, sql, relationName, dependentOn: JSON.parse(dependentOn), lineageIds: JSON.parse(lineageIds), parsedLogic: JSON.parse(parsedLogic), statementRefs: JSON.parse(statementRefs)  });
        });
        
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error(''));
      }
    }  
  
    all = async (targetOrgId: string, auth: Auth): Promise<Logic[]> => {
      try {
        const queryText = `select * from cito.lineage.${this.#matName};`;
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds:[] },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
        if (result.value.length !== 1)
          throw new Error(`Multiple or no logic entities with id found`);
  
        
  
          return result.value.map((el) => {
            const {
              ID: id,
              RELATION_NAME: relationName,
              SQL: sql,
              DEPENDENT_ON: dependentOn,
              PARSED_LOGIC: parsedLogic,
              STATEMENT_REFS: statementRefs,
              LINEAGE_IDS: lineageIds,
            } = el;
      
            if (
              typeof id !== 'string' ||
              typeof relationName !== 'string' ||
              typeof sql !== 'string' ||
              typeof dependentOn !== 'string' ||
              typeof parsedLogic !== 'string' ||
              typeof statementRefs !== 'string' ||
              typeof lineageIds !== 'string' 
            )
              throw new Error(
                'Retrieved unexpected logic field types from persistence'
              );
    
            return this.#toEntity({ id, sql, relationName, dependentOn: JSON.parse(dependentOn), lineageIds: JSON.parse(lineageIds), parsedLogic: JSON.parse(parsedLogic), statementRefs: JSON.parse(statementRefs)  });
          });
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error(''));
      }
    };
  
    insertOne = async (
      logic: Logic,
      targetOrgId: string,
      auth: Auth
    ): Promise<string> => {
      const colDefinitions: ColumnDefinition[] = [
        { name: 'target_resource_ids', selectType: 'parse_json' },

        { name: 'id' },
        { name: 'created_at' },
        { name: 'completed' },
      ];
  
      const row = `(?, ?, ?)`;
      const binds = [logic.id, logic.createdAt, logic.completed.toString()];
  
      try {
        const queryText = getInsertQuery(this.#matName, colDefinitions, [row]);
  
        const result = await this.#querySnowflake.execute(
          { queryText, targetOrgId, binds },
          auth
        );
  
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error('Missing sf query value');
  
        return logic.id;
      } catch (error: unknown) {
        if (error instanceof Error && error.message) console.trace(error.message);
        else if (!(error instanceof Error) && error) console.trace(error);
        return Promise.reject(new Error(''));
      }
    };
  
  
    #toEntity = (logicProperties: LogicProps): Logic =>
      Logic.build(logicProperties);
  }
  











  

  



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
        .

      if (!result.acknowledged)
        throw new Error('Logic delete failed. Delete not acknowledged');

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error(''));
    }
  };

  #toEntity = (logicProperties: LogicProps): Logic =>
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

  #buildProperties = (logic: LogicPersistence): LogicProps => ({
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
    logicIds: logic.logicIds,
    organizationId: logic.organizationId,
  });

  #toPersistence = (logic: Logic): Document => ({
    _id: ObjectId.createFromHexString(logic.id),
    relationName: logic.relationName,
    sql: logic.sql,
    dependentOn: logic.dependentOn,
    parsedLogic: logic.parsedLogic,
    statementRefs: logic.statementRefs,
    logicIds: logic.logicIds,
    organizationId: logic.organizationId,
  });
}
