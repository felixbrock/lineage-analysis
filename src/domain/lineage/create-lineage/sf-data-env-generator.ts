import { CreateColumn } from '../../column/create-column';
import {
  Column,
  ColumnDataType,
  parseColumnDataType,
} from '../../entities/column';
import { Logic } from '../../entities/logic';
import {
  Materialization,
  MaterializationType,
  parseMaterializationType,
} from '../../entities/materialization';
import { QuerySnowflake } from '../../integration-api/snowflake/query-snowflake';
import { CreateMaterialization } from '../../materialization/create-materialization';
import { DbConnection } from '../../services/i-db';

interface Auth {
  jwt: string;
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export interface DataEnvProps {
  lineageId: string;
  targetOrganizationId?: string;
}

export interface GenerateResult {
  materializations: Materialization[];
  columns: Column[];
  logics: Logic[];
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

interface MaterializationRepresentation {
  databaseName: string;
  schemaName: string;
  name: string;
  type: MaterializationType;
  ownerId?: string;
  isTransient?: boolean;
  comment?: string;
}

export class SfDataEnvGenerator {
  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #querySnowflake: QuerySnowflake;

  readonly #lineageId: string;

  readonly #targetOrganizationId?: string;

  readonly #auth: Auth;

  readonly #organizationId: string;

  readonly #dbConnection;

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

  constructor(
    props: DataEnvProps,
    auth: Auth,
    dbConnection: DbConnection,
    dependencies: {
      createMaterialization: CreateMaterialization;
      createColumn: CreateColumn;
      querySnowflake: QuerySnowflake;
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

    this.#querySnowflake = dependencies.querySnowflake;
    this.#createMaterialization = dependencies.createMaterialization;
    this.#createColumn = dependencies.createColumn;

    this.#auth = auth;
    this.#dbConnection = dbConnection;

    this.#lineageId = props.lineageId;
    this.#targetOrganizationId = props.targetOrganizationId;
  }

  /* Get materialization representations from snowflake */
  #getMatRepresentations = async (): Promise<
    MaterializationRepresentation[]
  > => {
    const query =
      'select table_catalog, table_schema, table_name, table_owner, table_type, is_transient, comment  from cito.information_schema.tables';
    const queryResult = await this.#querySnowflake.execute(
      { query, targetOrganizationId: this.#targetOrganizationId },
      this.#auth,
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const matRepresentations: MaterializationRepresentation[] = results[this.#organizationId].map(
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
          ownerId,
          type: parseMaterializationType(type.toLowerCase()),
          isTransient,
          comment,
        };
      }
    );

    return matRepresentations;
  };

  /* Get column representations from snowflake */
  #getColumnRepresentations = async (): Promise<ColumnRepresentation[]> => {
    const query =
      'select table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, is_identity, comment from cito.information_schema.columns limit 10';
    const queryResult = await this.#querySnowflake.execute(
      { query, targetOrganizationId: this.#targetOrganizationId },
      this.#auth,
      
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const columnRepresentations: ColumnRepresentation[] = results[this.#organizationId].map((el) => {
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
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      this.#auth,
      this.#dbConnection
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new SyntaxError(`Creation of column failed`);

    return createColumnResult.value;
  };

  /* Creates materialization object and its column objects */
  #generateDWResources = async (
    resourceProps: {
      matRepresentation: MaterializationRepresentation;
      columnRepresentations: ColumnRepresentation[];
      relationName: string;
    },
    options: { writeToPersistence: boolean }
  ): Promise<void> => {
    const { matRepresentation, columnRepresentations } = resourceProps;

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          ...matRepresentation,
          relationName: resourceProps.relationName,
          writeToPersistence: options.writeToPersistence,
          lineageId: this.#lineageId,
          targetOrganizationId: this.#targetOrganizationId,
        },
        this.#auth,
        this.#dbConnection
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

  /* Runs through dbt nodes and creates objects like logic, materializations and columns */
  generate = async (): Promise<GenerateResult> => {
    const matRepresentations = await this.#getMatRepresentations();
    const columnRepresentations = await this.#getColumnRepresentations();

    await Promise.all(
      matRepresentations.map(async (el) => {
        const options = {
          writeToPersistence: false,
        };

        const relationName = `${el.databaseName}.${el.schemaName}.${el.name}`;

        await this.#generateDWResources(
          {
            matRepresentation: el,
            columnRepresentations: columnRepresentations.filter(
              (colRep) => colRep.relationName === relationName
            ),
            relationName,
          },
          options
        );
      })
    );

    return {
      materializations: this.#materializations,
      columns: this.#columns,
      logics: this.#logics,
    };
  };
}
