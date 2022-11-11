import { CreateLogic } from '../../logic/create-logic';
import { CreateColumn } from '../../column/create-column';
import {
  Column,
  ColumnDataType,
  parseColumnDataType,
} from '../../entities/column';
import { Logic, ModelRepresentation } from '../../entities/logic';
import {
  Materialization,
  MaterializationType,
  parseMaterializationType,
} from '../../entities/materialization';
import { CreateMaterialization } from '../../materialization/create-materialization';
import {  } from '../../services/i-db';
import { ParseSQL, ParseSQLResponseDto } from '../../sql-parser-api/parse-sql';
import { GenerateResult, IDataEnvGenerator } from './i-data-env-generator';
import { QuerySnowflake } from '../../snowflake-api/query-snowflake';

interface Auth {
  jwt: string;
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export interface DataEnvProps {
  lineageId: string;
  targetOrganizationId?: string;
}

interface ColumnRepresentation {
  relationName: string;
  name: string;
  dataType: ColumnDataType;
  index: string;
  isIdentity?: boolean;
  isNullable?: boolean;
  comment?: string;
}

interface DatabaseRepresentation {
  name: string;
  ownerId: string;
  isTransient: boolean;
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

export class SfDataEnvGenerator implements IDataEnvGenerator {
  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createLogic: CreateLogic;

  readonly #querySnowflake: QuerySnowflake;

  readonly #parseSQL: ParseSQL;

  readonly #lineageId: string;

  readonly #targetOrgId?: string;

  readonly #auth: Auth;

  readonly #organizationId: string;

  #materializations: Materialization[] = [];

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

  constructor(
    props: DataEnvProps,
    auth: Auth,
    dependencies: {
      createMaterialization: CreateMaterialization;
      createColumn: CreateColumn;
      createLogic: CreateLogic;
      querySnowflake: QuerySnowflake;
      parseSQL: ParseSQL;
    }
  ) {
    if (auth.isSystemInternal && !props.targetOrganizationId)
      throw new Error('Target organization id missing');
    if (!auth.isSystemInternal && !auth.callerOrganizationId)
      throw new Error('Caller organization id missing');
    if (!props.targetOrganizationId && !auth.callerOrganizationId)
      throw new Error('No organization Id instance provided');
    if (props.targetOrganizationId && auth.callerOrganizationId)
      throw new Error('callerOrgId and targetOrgId provided. Not allowed');

    if (auth.callerOrganizationId)
      this.#organizationId = auth.callerOrganizationId;
    else if (props.targetOrganizationId)
      this.#organizationId = props.targetOrganizationId;
    else throw new Error('Missing orgId');

    this.#createMaterialization = dependencies.createMaterialization;
    this.#createColumn = dependencies.createColumn;
    this.#createLogic = dependencies.createLogic;
    this.#querySnowflake = dependencies.querySnowflake;
    this.#parseSQL = dependencies.parseSQL;

    this.#auth = auth;

    this.#lineageId = props.lineageId;
    this.#targetOrgId = props.targetOrganizationId;
  }

  /* Get database representations from snowflake */
  #getDbRepresentations = async (): Promise<
  DatabaseRepresentation[]
> => {
  const binds = [targetDbName];
  const queryText =
    'select database_name, database_owner, is_transient, comment from cito.information_schema.databases';
  const queryResult = await this.#querySnowflake.execute(
    { queryText, targetOrgId: this.#targetOrgId, binds },
    this.#auth
  );
  if (!queryResult.success) {
    throw new Error(queryResult.error);
  }
  if (!queryResult.value) throw new Error('Query did not return a value');

  const results = queryResult.value;

  const matRepresentations: MaterializationRepresentation[] = results.map((el) => {
    const {
      TABLE_CATALOG: databaseName,
      TABLE_SCHEMA: schemaName,
      TABLE_NAME: name,
      TABLE_OWNER: ownerId,
      TABLE_TYPE: type,
      IS_TRANSIENT: isTransient,
      COMMENT: comment,
    } = el;

    if (
      typeof databaseName !== 'string' ||
      typeof schemaName !== 'string' ||
      typeof name !== 'string' ||
      typeof ownerId !== 'string' ||
      typeof type !== 'string' ||
      typeof isTransient !== 'boolean' ||
      typeof comment !== 'string'
    )
      throw new Error(
        'Received mat representation field value in unexpected format'
      );

    return {
      databaseName,
      schemaName,
      name,
      relationName: `${el.databaseName}.${el.schemaName}.${el.name}`,
      ownerId,
      type: parseMaterializationType(type.toLowerCase()),
      isTransient,
      comment,
    };
  });

  return matRepresentations;
};

  /* Get materialization representations from snowflake */
  #getMatRepresentations = async (targetDbName: string): Promise<
    MaterializationRepresentation[]
  > => {
    const binds = [targetDbName];
    const queryText =
      'select table_catalog, table_schema, table_name, table_owner, table_type, is_transient, comment  from ?.information_schema.tables';
    const queryResult = await this.#querySnowflake.execute(
      { queryText, targetOrgId: this.#targetOrgId, binds },
      this.#auth
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const matRepresentations: MaterializationRepresentation[] = results.map((el) => {
      const {
        TABLE_CATALOG: databaseName,
        TABLE_SCHEMA: schemaName,
        TABLE_NAME: name,
        TABLE_OWNER: ownerId,
        TABLE_TYPE: type,
        IS_TRANSIENT: isTransient,
        COMMENT: comment,
      } = el;

      if (
        typeof databaseName !== 'string' ||
        typeof schemaName !== 'string' ||
        typeof name !== 'string' ||
        typeof ownerId !== 'string' ||
        typeof type !== 'string' ||
        typeof isTransient !== 'boolean' ||
        typeof comment !== 'string'
      )
        throw new Error(
          'Received mat representation field value in unexpected format'
        );

      return {
        databaseName,
        schemaName,
        name,
        relationName: `${el.databaseName}.${el.schemaName}.${el.name}`,
        ownerId,
        type: parseMaterializationType(type.toLowerCase()),
        isTransient,
        comment,
      };
    });

    return matRepresentations;
  };

  /* Get logic representations from Snowflake */
  #getLogicRepresentation = async (
    ddlObjectType: 'table' | 'view',
    relationName: string
  ): Promise<LogicRepresentation> => {
    const binds = [ddlObjectType, relationName];
    const queryText = `select get_ddl(?, ?, true) as sql`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, targetOrgId: this.#targetOrgId, binds },
      this.#auth
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    if (results.length !== 1)
      throw new Error('No or multiple sql logic instances returned for mat');

    const { SQL: sql } = results[0];

    if (typeof sql !== 'string')
      throw new Error(
        'Received mat representation field value in unexpected format'
      );

    return { sql };
  };

  /* Get column representations from snowflake */
  #getColumnRepresentations = async (targetDbName: string): Promise<ColumnRepresentation[]> => {
    const binds = [targetDbName];
    const queryText =
      'select table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, is_identity, comment from ?.information_schema.columns';
    const queryResult = await this.#querySnowflake.execute(
      { queryText, targetOrgId: this.#targetOrgId, binds },
      this.#auth
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

      if (
        typeof databaseName !== 'string' ||
        typeof schemaName !== 'string' ||
        typeof matName !== 'string' ||
        typeof name !== 'string' ||
        typeof index !== 'number' ||
        typeof isNullable !== 'boolean' ||
        typeof dataType !== 'string' ||
        typeof isIdentity !== 'boolean' ||
        typeof comment !== 'string'
      )
        throw new Error(
          'Received column representation field value in unexpected format'
        );

      return {
        relationName: `${databaseName}.${schemaName}.${matName}`,
        name,
        index: index.toString(),
        dataType: parseColumnDataType(dataType),
        isIdentity,
        isNullable,
        comment,
      };
    });

    return columnRepresentations;
  };

  /* Sends sql to parse SQL microservices and receives parsed SQL logic back */
  #parseLogic = async (sql: string): Promise<string> => {
    const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute({
      dialect: 'snowflake',
      sql,
    });

    if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
    if (!parseSQLResult.value)
      throw new SyntaxError(`Parsing of SQL logic failed`);

    return JSON.stringify(parseSQLResult.value);
  };

  #generateColumn = async (
    columnRepresentation: ColumnRepresentation,
    matId: string
  ): Promise<Column> => {
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
        targetOrganizationId: this.#targetOrgId,
        writeToPersistence: false,
      },
      this.#auth,
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
    const parsedLogic = await this.#parseLogic(logicRepresentation.sql);

    const createLogicResult = await this.#createLogic.execute(
      {
        props: {
          generalProps: {
            relationName,
            sql: logicRepresentation.sql,
            lineageId: this.#lineageId,
            parsedLogic,
            targetOrganizationId: this.#targetOrgId,
            catalog: this.#catalog,
          },
        },
        options: {
          writeToPersistence: false,
        },
      },
      this.#auth,
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
          targetOrganizationId: this.#targetOrgId,
        },
        this.#auth,
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

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  generate = async (): Promise<GenerateResult> => {
    const accessibleDbs = await.

    const matRepresentations = await this.#getMatRepresentations();
    const columnRepresentations = await this.#getColumnRepresentations();

    const colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    } = columnRepresentations.reduce(
      SfDataEnvGenerator.#groupByRelationName,
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
          el.relationName
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

    return {
      materializations: this.#materializations,
      columns: this.#columns,
      logics: this.#logics,
      catalog: this.#catalog,
    };
  };
}
