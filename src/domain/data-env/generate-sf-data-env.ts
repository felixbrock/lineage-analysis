// todo clean architecture violation
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { DataEnv } from './data-env';
import {
  Column,
  ColumnDataType,
  parseColumnDataType,
} from '../entities/column';
import {
  Materialization,
  MaterializationType,
  parseMaterializationType,
} from '../entities/materialization';
import { CreateMaterialization } from '../materialization/create-materialization';
import { CreateColumn } from '../column/create-column';
import { CreateLogic } from '../logic/create-logic';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { ParseSQL } from '../sql-parser-api/parse-sql';
import { Logic, ModelRepresentation } from '../entities/logic';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseGetSfDataEnv from './base-get-sf-data-env';

export interface GenerateSfDataEnvRequestDto {
  lineageId: string;
}

export interface GenerateSfDataEnvAuthDto
  extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type GenerateSfDataEnvResponse = Result<DataEnv>;

interface ColumnRepresentation {
  relationName: string;
  name: string;
  dataType: ColumnDataType;
  index: string;
  isIdentity?: boolean;
  isNullable?: boolean;
  comment?: string;
}

interface MaterializationRepresentation {
  databaseName: string;
  schemaName: string;
  name: string;
  relationName: string;
  type: MaterializationType;
  ownerId?: string;
  isTransient?: boolean;
  comment?: string;
}

interface LogicRepresentation {
  sql: string;
}

export class GenerateSfDataEnv extends BaseGetSfDataEnv
  implements
    IUseCase<
      GenerateSfDataEnvRequestDto,
      GenerateSfDataEnvResponse,
      GenerateSfDataEnvAuthDto
    >
{
  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createLogic: CreateLogic;

  readonly #querySnowflake: QuerySnowflake;

  readonly #parseSQL: ParseSQL;

  readonly #materializations: Materialization[] = [];

  get materializations(): Materialization[] {
    return this.#materializations;
  }

  #columns: Column[] = [];

  get columns(): Column[] {
    return this.#columns;
  }

  #logics: Logic[] = [];

  get logics(): Logic[] {
    return this.#logics;
  }

  #catalog: ModelRepresentation[] = [];

  #connPool?: IConnectionPool;

  #auth?: GenerateSfDataEnvAuthDto;

  #req?: GenerateSfDataEnvRequestDto;

  #lineageId?: string;

  constructor(
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createLogic: CreateLogic,
    querySnowflake: QuerySnowflake,
    parseSQL: ParseSQL
  ) {
    super(querySnowflake);
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createLogic = createLogic;
    this.#querySnowflake = querySnowflake;
    this.#parseSQL = parseSQL;
  }

  

  /* Get materialization representations from snowflake */
  #getMatRepresentations = async (
    targetDbName: string
  ): Promise<MaterializationRepresentation[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select table_catalog, table_schema, table_name, table_owner, table_type, is_transient, comment  from ${targetDbName}.information_schema.tables where table_schema != upper('information_schema');`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      this.#auth,
      this.#connPool
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

  /* Get logic representations from Snowflake */
  #getLogicRepresentation = async (
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

  /* Get column representations from snowflake */
  #getColumnRepresentations = async (
    targetDbName: string
  ): Promise<ColumnRepresentation[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, is_identity, comment from ${targetDbName}.information_schema.columns where table_schema != uppper('information_schema')`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      this.#auth,
      this.#connPool
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

  #generateColumn = async (
    columnRepresentation: ColumnRepresentation,
    matId: string
  ): Promise<Column> => {
    if (!this.#connPool || !this.#lineageId || !this.#auth)
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
        lineageId: this.#lineageId,
        writeToPersistence: false,
      },
      this.#auth,
      this.#connPool
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new SyntaxError(`Creation of column failed`);

    return createColumnResult.value;
  };

  #generateLogic = async (
    logicRepresentation: LogicRepresentation,
    relationName: string
  ): Promise<Logic> => {
    if (!this.#connPool || !this.#lineageId || !this.#auth)
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
            lineageId: this.#lineageId,
            parsedLogic,
            catalog: this.#catalog,
          },
        },
        options: {
          writeToPersistence: false,
        },
      },
      this.#auth,
      this.#connPool
    );

    if (!createLogicResult.success) throw new Error(createLogicResult.error);
    if (!createLogicResult.value)
      throw new SyntaxError(`Creation of logic failed`);

    const logic = createLogicResult.value;

    this.#logics.push(logic);

    return logic;
  };

  /* Creates materialization object and its column objects */
  #generateDWResource = async (
    resourceProps: {
      matRepresentation: MaterializationRepresentation;
      logicRepresentation: LogicRepresentation;
      columnRepresentations: ColumnRepresentation[];
      relationName: string;
    },
    options: { writeToPersistence: boolean }
  ): Promise<void> => {
    if (!this.#connPool || !this.#lineageId || !this.#auth)
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
          lineageId: this.#lineageId,
        },
        this.#auth,
        this.#connPool
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

    this.#materializations.push(materialization);
    this.#columns.push(...generatedColumns);
  };

  #generateCatalog = (
    matRepresentations: MaterializationRepresentation[],
    colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    }
  ): void => {
    const modelRepresentations = matRepresentations.map(
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

    this.#catalog.push(...modelRepresentations);
  };

  static #groupByRelationName = <T extends { relationName: string }>(
    accumulation: { [key: string]: T[] },
    element: T
  ): { [key: string]: T[] } => {
    const localAcc = accumulation;

    const key = element.relationName;
    if (!(key in accumulation)) {
      localAcc[key] = [];
    }
    localAcc[key].push(element);
    return localAcc;
  };

  #generateDbResources = async (dbName: string): Promise<void> => {
    const matRepresentations = await this.#getMatRepresentations(dbName);
    const columnRepresentations = await this.#getColumnRepresentations(dbName);

    const colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    } = columnRepresentations.reduce(
      GenerateSfDataEnv.#groupByRelationName,
      {}
    );

    this.#generateCatalog(matRepresentations, colRepresentationsByRelationName);

    await Promise.all(
      matRepresentations.map(async (el) => {
        const options = {
          writeToPersistence: false,
        };

        const logicRepresentation = await this.#getLogicRepresentation(
          el.type === 'view' ? 'view' : 'table',
          el.name,
          el.schemaName,
          el.databaseName
        );

        await this.#generateDWResource(
          {
            matRepresentation: el,
            logicRepresentation,
            columnRepresentations:
              colRepresentationsByRelationName[el.relationName],
            relationName: el.relationName,
          },
          options
        );
      })
    );
  };

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateSfDataEnvRequestDto,
    auth: GenerateSfDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<GenerateSfDataEnvResponse> {
    try {
      this.#connPool = connPool;

      const dbRepresentations = await this.getDbRepresentations(connPool, auth);

      await Promise.all(
        dbRepresentations.map(async (el) => {
          await this.#generateDbResources(el.name);
        })
      );

      return Result.ok({
        matsToCreate: this.#materializations,
        columnsToCreate: this.#columns,
        logicsToCreate: this.#logics,
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
