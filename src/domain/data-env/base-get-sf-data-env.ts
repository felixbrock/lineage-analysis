import { CreateColumn } from '../column/create-column';
import { Column, ColumnDataType, parseColumnDataType } from '../entities/column';
import { Logic, ModelRepresentation } from '../entities/logic';
import { Materialization, MaterializationType, parseMaterializationType } from '../entities/materialization';
import { CreateLogic } from '../logic/create-logic';
import { CreateMaterialization } from '../materialization/create-materialization';
import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { ParseSQL } from '../sql-parser-api/parse-sql';

export interface DatabaseRepresentation {
  name: string;
  ownerId: string;
  isTransient: boolean;
  comment?: string;
}

export interface ColumnRepresentation {
  relationName: string;
  name: string;
  dataType: ColumnDataType;
  index: string;
  isIdentity?: boolean;
  isNullable?: boolean;
  comment?: string;
}

export interface MaterializationRepresentation {
  databaseName: string;
  schemaName: string;
  name: string;
  relationName: string;
  type: MaterializationType;
  ownerId?: string;
  isTransient?: boolean;
  comment?: string;
}

export interface LogicRepresentation {
  sql: string;
}

interface Auth extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export default abstract class BaseGetSfDataEnv {
  readonly #querySnowflake: QuerySnowflake;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createLogic: CreateLogic;

  readonly #parseSQL: ParseSQL;

  protected readonly materializations: Materialization[] = [];

  protected readonly columns: Column[] = [];

  protected readonly logics: Logic[] = [];

  protected readonly catalog: ModelRepresentation[] = [];

  protected connPool?: IConnectionPool;

  protected auth?: Auth;

  protected lineageId?: string;

  constructor(
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createLogic: CreateLogic,
    querySnowflake: QuerySnowflake,
    parseSQL: ParseSQL
  ) {
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createLogic = createLogic;
    this.#querySnowflake = querySnowflake;
    this.#parseSQL = parseSQL;
  }

  /* Get database representations from snowflake */
  protected getDbRepresentations = async (
    connPool: IConnectionPool,
    auth: BaseAuth
  ): Promise<DatabaseRepresentation[]> => {
    const queryText =
      "select database_name, database_owner, is_transient, comment from cito.information_schema.databases where not array_contains(database_name::variant, array_construct(upper('snowflake'), upper('snowflake_sample_data'), upper('cito')))";
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      auth,
      connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dbRepresentations: DatabaseRepresentation[] = results.map((el) => {
      const {
        DATABASE_NAME: name,
        DATABASE_OWNER: ownerId,
        IS_TRANSIENT: isTransient,
        COMMENT: comment,
      } = el;

      const isComment = (val: unknown): val is string | undefined =>
        !val || typeof val === 'string';

      if (
        typeof name !== 'string' ||
        typeof ownerId !== 'string' ||
        typeof isTransient !== 'string' ||
        !['yes', 'no'].includes(isTransient.toLowerCase()) ||
        !isComment(comment)
      )
        throw new Error(
          'Received mat representation field value in unexpected format'
        );

      return {
        name: name.toLowerCase(),
        ownerId,
        isTransient: isTransient.toLowerCase() !== 'no',
        comment: comment || undefined,
      };
    });

    return dbRepresentations;
  };

  /* Get materialization representations from snowflake */
  protected getMatRepresentations = async (
    targetDbName: string
  ): Promise<MaterializationRepresentation[]> => {
    if (!this.connPool || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select table_catalog, table_schema, table_name, table_owner, table_type, is_transient, comment  from ${targetDbName}.information_schema.tables where table_schema != upper('information_schema');`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      this.auth,
      this.connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const matRepresentations: MaterializationRepresentation[] = results.map(
      (el) => {
        const {
          TABLE_CATALOG: databaseName,
          TABLE_SCHEMA: schemaName,
          TABLE_NAME: name,
          TABLE_OWNER: ownerId,
          TABLE_TYPE: type,
          IS_TRANSIENT: isTransient,
          COMMENT: comment,
        } = el;

        const isComment = (val: unknown): val is string | undefined =>
          !val || typeof val === 'string';
        const isOwnerId = (val: unknown): val is string | undefined =>
          !val || typeof val === 'string';
        const isIsTransientVal = (val: unknown): val is string | undefined =>
          !val ||
          (typeof val === 'string' &&
            ['yes', 'no'].includes(val.toLowerCase()));

        if (
          typeof databaseName !== 'string' ||
          typeof schemaName !== 'string' ||
          typeof name !== 'string' ||
          typeof type !== 'string' ||
          !isIsTransientVal(isTransient) ||
          !isComment(comment) ||
          !isOwnerId(ownerId)
        )
          throw new Error(
            'Received mat representation field value in unexpected format'
          );

        const dbNameFormatted = databaseName.toLowerCase();
        const schemaNameFormatted = schemaName.toLowerCase();
        const nameFormatted = name.toLowerCase();

        return {
          databaseName: dbNameFormatted,
          schemaName: schemaNameFormatted,
          name: nameFormatted,
          relationName: `${dbNameFormatted}.${schemaNameFormatted}.${nameFormatted}`,
          type: parseMaterializationType(type.toLowerCase()),
          ownerId: ownerId || undefined,
          isTransient: isTransient
            ? isTransient.toLowerCase() !== 'no'
            : undefined,
          comment: comment || undefined,
        };
      }
    );

    return matRepresentations;
  };

  /* Get column representations from snowflake */
  protected getColumnRepresentations = async (
    targetDbName: string
  ): Promise<ColumnRepresentation[]> => {
    if (!this.connPool || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, is_identity, comment from ${targetDbName}.information_schema.columns where table_schema != uppper('information_schema')`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      this.auth,
      this.connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const columnRepresentations: ColumnRepresentation[] = results.map((el) => {
      const {
        TABLE_CATALOG: databaseName,
        TABLE_SCHEMA: schemaName,
        TABLE_NAME: matName,
        COLUMN_NAME: name,
        ORDINAL_POSITION: index,
        IS_NULLABLE: isNullable,
        DATA_TYPE: dataType,
        IS_IDENTITY: isIdentity,
        COMMENT: comment,
      } = el;

      const isIsIdentityVal = (val: unknown): val is string | undefined =>
        !val ||
        (typeof val === 'string' && ['yes', 'no'].includes(val.toLowerCase()));
      const isIsNullableVal = (val: unknown): val is string | undefined =>
        !val ||
        (typeof val === 'string' && ['yes', 'no'].includes(val.toLowerCase()));
      const isComment = (val: unknown): val is string | undefined =>
        !val || typeof val === 'string';

      if (
        typeof databaseName !== 'string' ||
        typeof schemaName !== 'string' ||
        typeof matName !== 'string' ||
        typeof name !== 'string' ||
        typeof index !== 'number' ||
        typeof dataType !== 'string' ||
        !isIsNullableVal(isNullable) ||
        !isIsIdentityVal(isIdentity) ||
        !isComment(comment)
      )
        throw new Error(
          'Received column representation field value in unexpected format'
        );

      return {
        relationName: `${databaseName.toLowerCase()}.${schemaName.toLowerCase()}.${matName.toLowerCase()}`,
        name: name.toLowerCase(),
        index: index.toString(),
        dataType: parseColumnDataType(dataType),
        isIdentity: isIdentity ? isIdentity.toLowerCase() !== 'no' : undefined,
        isNullable: isNullable ? isNullable.toLowerCase() !== 'no' : undefined,
        comment: comment || undefined,
      };
    });

    return columnRepresentations;
  };

  #generateColumn = async (
    columnRepresentation: ColumnRepresentation,
    matId: string
  ): Promise<Column> => {
    if (!this.connPool || !this.lineageId || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const createColumnResult = await this.#createColumn.execute(
      {
        relationName: columnRepresentation.relationName,
        name: columnRepresentation.name,
        index: columnRepresentation.index,
        dataType: columnRepresentation.dataType,
        materializationId: matId,
        isIdentity: columnRepresentation.isIdentity,
        isNullable: columnRepresentation.isNullable,
        comment: columnRepresentation.comment,
        lineageId: this.lineageId,
        writeToPersistence: false,
      },
      this.auth,
      this.connPool
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new SyntaxError(`Creation of column failed`);

    return createColumnResult.value;
  };

  /* Get logic representations from Snowflake */
  protected getLogicRepresentation = async (
    ddlObjectType: 'table' | 'view',
    matName: string,
    schemaName: string,
    dbName: string
  ): Promise<LogicRepresentation> => {
    const foo = 'Lineage';
    return {
      sql: `${foo} SQL model placeholder for ${ddlObjectType} ${dbName}.${schemaName}.${matName}`,
    };

    // const binds = [ddlObjectType, `${dbName}.${schemaName}.${matName}`];
    // const queryText = `select get_ddl(?, ?, true) as sql`;
    // const queryResult = await this.#querySnowflake.execute(
    //   { queryText, binds, profile: this.#profile },
    //   this.#auth
    // );
    // if (!queryResult.success) {
    //   throw new Error(queryResult.error);
    // }
    // if (!queryResult.value) throw new Error('Query did not return a value');

    // const results = queryResult.value;

    // if (results.length !== 1)
    //   throw new Error('No or multiple sql logic instances returned for mat');

    // const { SQL: sql } = results[0];

    // if (typeof sql !== 'string')
    //   throw new Error(
    //     'Received mat representation field value in unexpected format'
    //   );

    // return { sql };
  };

  /* Sends sql to parse SQL microservices and receives parsed SQL logic back */
  // #parseLogic = async (sql: string): Promise<string> => {
  //   const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute({
  //     dialect: 'snowflake',
  //     sql,
  //   });

  //   if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
  //   if (!parseSQLResult.value)
  //     throw new SyntaxError(`Parsing of SQL logic failed`);

  //   return JSON.stringify(parseSQLResult.value);
  // };

  #generateLogic = async (
    logicRepresentation: LogicRepresentation,
    relationName: string
  ): Promise<Logic> => {
    if (!this.connPool || !this.lineageId || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    // const parsedLogic = logicRepresentation.sql
    //   ? await this.#parseLogic(logicRepresentation.sql)
    //   : '';

    const parsedLogic = JSON.stringify({ file: [{}, {}] });

    const createLogicResult = await this.#createLogic.execute(
      {
        props: {
          generalProps: {
            relationName,
            sql: logicRepresentation.sql,
            lineageId: this.lineageId,
            parsedLogic,
            catalog: this.catalog,
          },
        },
        options: {
          writeToPersistence: false,
        },
      },
      this.auth,
      this.connPool
    );

    if (!createLogicResult.success) throw new Error(createLogicResult.error);
    if (!createLogicResult.value)
      throw new SyntaxError(`Creation of logic failed`);

    const logic = createLogicResult.value;

    this.logics.push(logic);

    return logic;
  };

  protected generateCatalog = (
    matRepresentations: MaterializationRepresentation[],
    colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    }
  ): ModelRepresentation[] =>
    matRepresentations.map(
      (el): ModelRepresentation => ({
        relationName: el.relationName,
        materializationName: el.name,
        schemaName: el.schemaName,
        databaseName: el.databaseName,
        columnNames: colRepresentationsByRelationName[el.relationName].map(
          (colRep) => colRep.name
        ),
      })
    );

  /* Creates materialization object and its column objects */
  protected generateDWResource = async (
    resourceProps: {
      matRepresentation: MaterializationRepresentation;
      logicRepresentation: LogicRepresentation;
      columnRepresentations: ColumnRepresentation[];
      relationName: string;
    },
    options: { writeToPersistence: boolean }
  ): Promise<void> => {
    if (!this.connPool || !this.lineageId || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const { matRepresentation, columnRepresentations, logicRepresentation } =
      resourceProps;

    const { id: logicId } = await this.#generateLogic(
      logicRepresentation,
      resourceProps.relationName
    );

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          ...matRepresentation,
          relationName: resourceProps.relationName,
          writeToPersistence: options.writeToPersistence,
          logicId,
          lineageId: this.lineageId,
        },
        this.auth,
        this.connPool
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    const generatedColumns = await Promise.all(
      columnRepresentations.map(async (el) =>
        this.#generateColumn(el, materialization.id)
      )
    );

    this.materializations.push(materialization);
    this.columns.push(...generatedColumns);
  };

  protected abstract generateDbResources(base: unknown):Promise<void>;
}
