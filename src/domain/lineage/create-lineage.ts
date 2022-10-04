// todo - Clean Architecture dependency violation. Fix
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn } from '../column/create-column';
import {
  CreateMaterialization,
  CreateMaterializationRequestDto,
} from '../materialization/create-materialization';
import { CreateLogic } from '../logic/create-logic';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import {
  Logic,
  ColumnRef,
  Refs,
  MaterializationDefinition,
  DashboardRef,
} from '../entities/logic';
import {
  CreateDependency,
  CreateDependencyResponse,
} from '../dependency/create-dependency';
import { Dependency, DependencyType } from '../entities/dependency';
import { ReadColumns } from '../column/read-columns';
import {
  Materialization,
  MaterializationType,
} from '../entities/materialization';
import { Column } from '../entities/column';
import { ILineageRepo } from './i-lineage-repo';
import { IColumnRepo } from '../column/i-column-repo';
import { IMaterializationRepo } from '../materialization/i-materialization-repo';
import { IDependencyRepo } from '../dependency/i-dependency-repo';
import { ILogicRepo } from '../logic/i-logic-repo';
import { DbConnection } from '../services/i-db';
import {
  QuerySnowflakeHistory,
  QueryHistoryResponseDto,
} from '../query-snowflake-history-api/query-snowflake-history';
import { Dashboard } from '../entities/dashboard';
import { CreateExternalDependency } from '../dependency/create-external-dependency';
import { IDashboardRepo } from '../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../dashboard/create-dashboard';
import { BiLayer, parseBiLayer } from '../value-types/bilayer';

export interface CreateLineageRequestDto {
  lineageId?: string;
  lineageCreatedAt?: string;
  targetOrganizationId?: string;
  catalog: string;
  manifest: string;
  biType?: string;
}

export interface CreateLineageAuthDto {
  jwt: string;
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateLineageResponseDto = Result<Lineage>;

interface DbtResources {
  nodes: any;
  sources: any;

  parent_map: { [key: string]: string[] };
}

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto,
      DbConnection
    >
{
  readonly #createLogic: CreateLogic;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createDashboard: CreateDashboard;

  readonly #createDependency: CreateDependency;

  readonly #createExternalDependency: CreateExternalDependency;

  readonly #parseSQL: ParseSQL;

  readonly #querySnowflakeHistory: QuerySnowflakeHistory;

  readonly #lineageRepo: ILineageRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  readonly #dashboardRepo: IDashboardRepo;

  readonly #readColumns: ReadColumns;

  #dbConnection: DbConnection;

  #lineage?: Lineage;

  #logics: Logic[];

  #materializations: Materialization[];

  #columns: Column[];

  #dependencies: Dependency[];

  #dashboards: Dashboard[];

  #lastQueryDependency?: ColumnRef;

  #matDefinitionCatalog: MaterializationDefinition[];

  #targetOrganizationId?: string;

  #callerOrganizationId?: string;

  #organizationId: string;

  #jwt: string;

  #isSystemInternal: boolean;

  constructor(
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    createExternalDependency: CreateExternalDependency,
    parseSQL: ParseSQL,
    querySnowflakeHistory: QuerySnowflakeHistory,
    lineageRepo: ILineageRepo,
    logicRepo: ILogicRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    dependencyRepo: IDependencyRepo,
    dashboardRepo: IDashboardRepo,
    readColumns: ReadColumns,
    createDashboard: CreateDashboard
  ) {
    this.#createLogic = createLogic;
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#createExternalDependency = createExternalDependency;
    this.#createDashboard = createDashboard;
    this.#parseSQL = parseSQL;
    this.#querySnowflakeHistory = querySnowflakeHistory;
    this.#lineageRepo = lineageRepo;
    this.#logicRepo = logicRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
    this.#dashboardRepo = dashboardRepo;
    this.#readColumns = readColumns;
    this.#logics = [];
    this.#materializations = [];
    this.#columns = [];
    this.#dependencies = [];
    this.#dashboards = [];
    this.#matDefinitionCatalog = [];
    this.#lineage = undefined;
    this.#lastQueryDependency = undefined;
    this.#targetOrganizationId = '';
    this.#callerOrganizationId = '';
    this.#organizationId = '';
    this.#jwt = '';
    this.#isSystemInternal = false;
  }

  /* Building a new lineage object that is referenced by resources like columns and materializations */
  #buildLineage = async (): Promise<void> => {
    // todo - enable lineage updating
    // this.#lineage =
    //   lineageId && lineageCreatedAt
    //     ? Lineage.create({
    //         id: lineageId,
    //         createdAt: lineageCreatedAt,
    //       })
    //     : Lineage.create({ id: new ObjectId().toHexString() });

    this.#lineage = Lineage.create({
      id: new ObjectId().toHexString(),
      organizationId: this.#organizationId,
    });
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
    columnDefinition: { name: string; index: string; type: string },
    sourceRelationName: string,
    sourceId: string
  ): Promise<Column> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    // todo - add additional properties like index
    const createColumnResult = await this.#createColumn.execute(
      {
        relationName: sourceRelationName,
        name: columnDefinition.name,
        index: columnDefinition.index,
        type: columnDefinition.type,
        materializationId: sourceId,
        lineageId: lineage.id,
        writeToPersistence: false,
        targetOrganizationId: this.#targetOrganizationId,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new SyntaxError(`Creation of column failed`);

    return createColumnResult.value;
  };

  /* Runs through manifest source objects and creates corresponding materialization objecst */
  #generateWarehouseSource = async (source: any): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          materializationType: source.metadata.type,
          name: source.metadata.name,
          relationName: source.relation_name,
          schemaName: source.metadata.schema,
          databaseName: source.metadata.database,
          logicId: 'todo - read from snowflake',
          lineageId: lineage.id,
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        {
          isSystemInternal: this.#isSystemInternal,
          callerOrganizationId: this.#callerOrganizationId,
        },
        this.#dbConnection
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    this.#materializations.push(materialization);

    const columns = await Promise.all(
      Object.keys(source.columns).map(async (columnKey) =>
        this.#generateColumn(
          source.columns[columnKey],
          materialization.relationName,
          materialization.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  /* Create materializations and columns that are referenced in SQL models, but are neither defined as node or source objects in manifest.json */
  #createExternalResources = async (
    statementRefs: Refs,
    dwMatDefinitions: MaterializationDefinition[]
  ): Promise<void> => {
    await Promise.all(
      dwMatDefinitions.map(async (def) => {
        const matchingMaterializations = statementRefs.materializations.filter(
          (el) =>
            (typeof def.databaseName === 'string' &&
            typeof el.databaseName === 'string'
              ? this.#insensitiveEquality(el.databaseName, def.databaseName)
              : def.databaseName === el.databaseName) &&
            (typeof def.schemaName === 'string' &&
            typeof el.schemaName === 'string'
              ? this.#insensitiveEquality(el.schemaName, def.schemaName)
              : def.schemaName === el.schemaName) &&
            this.#insensitiveEquality(el.name, def.materializationName)
        );

        if (matchingMaterializations.length === 0) return;
        if (matchingMaterializations.length > 1)
          throw new Error(
            'Multiple matching materialization refs in logic found'
          );

        const matRef = matchingMaterializations[0];
        if (!this.#lineage) throw new Error('Lineage object not available');

        const existingMat = this.#materializations.find(
          (el) => el.relationName === def.relationName
        );

        let mat = existingMat;
        if (!existingMat) {
          const createMaterializationResult =
            await this.#createMaterialization.execute(
              {
                materializationType: MaterializationType.TABLE,
                name: matRef.name,
                relationName: def.relationName,
                schemaName: matRef.schemaName || '',
                databaseName: matRef.databaseName || '',
                logicId: 'todo - read from snowflake',
                lineageId: this.#lineage.id,
                targetOrganizationId: this.#targetOrganizationId,
                writeToPersistence: false,
              },
              {
                isSystemInternal: this.#isSystemInternal,
                callerOrganizationId: this.#callerOrganizationId,
              },
              this.#dbConnection
            );

          if (!createMaterializationResult.success)
            throw new Error(createMaterializationResult.error);
          if (!createMaterializationResult.value)
            throw new SyntaxError(`Creation of materialization failed`);

          mat = createMaterializationResult.value;

          this.#materializations.push(mat);
        }

        const finalMat = mat;
        if (!finalMat)
          throw new Error(
            'Creating external resources -> Materialization obj not found'
          );

        const relevantColumnRefs = statementRefs.columns.filter((col) =>
          this.#insensitiveEquality(finalMat.name, col.materializationName)
        );

        const uniqueRelevantColumnRefs = relevantColumnRefs.filter(
          (column1, index, self) =>
            index ===
            self.findIndex((column2) =>
              this.#insensitiveEquality(column1.name, column2.name)
            )
        );

        const isColumn = (column: Column | undefined): column is Column =>
          !!column;

        const columns = (
          await Promise.all(
            uniqueRelevantColumnRefs.map(async (el) =>
              this.#generateColumn(
                { name: el.name, index: '-1', type: 'string' },
                finalMat.relationName,
                finalMat.id
              )
            )
          )
        ).filter(isColumn);

        this.#columns.push(...columns);

        // wildcards;
      })
    );
  };

  #generateNodeMaterialization = async (
    req: CreateMaterializationRequestDto
  ): Promise<Materialization> => {
    const createMaterializationResult =
      await this.#createMaterialization.execute(
        req,
        {
          isSystemInternal: this.#isSystemInternal,
          callerOrganizationId: this.#callerOrganizationId,
        },
        this.#dbConnection
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    this.#materializations.push(materialization);

    return materialization;
  };

  /* Generate dbt model node */
  #generateDbtModelNode = async (props: {
    model: any;
    modelManifest: any;
    dbtDependentOn: MaterializationDefinition[];
    catalogFile: string;
    lineageId: string;
  }): Promise<void> => {
    const sql = props.modelManifest.compiled_sql;

    const parsedLogic = await this.#parseLogic(sql);

    const createLogicResult = await this.#createLogic.execute(
      {
        relationName: props.modelManifest.relation_name,
        sql,
        modelName: props.model.metadata.name,
        dbtDependentOn: props.dbtDependentOn,
        lineageId: props.lineageId,
        parsedLogic,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
        catalogFile: props.catalogFile,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createLogicResult.success) throw new Error(createLogicResult.error);
    if (!createLogicResult.value)
      throw new SyntaxError(`Creation of logic failed`);

    const logic = createLogicResult.value;

    this.#logics.push(logic);

    await this.#createExternalResources(
      logic.statementRefs,
      logic.dependentOn.dwDependencyDefinitions
    );

    const mat = await this.#generateNodeMaterialization({
      materializationType: props.model.metadata.type,
      name: props.model.metadata.name,
      relationName: props.modelManifest.relation_name,
      schemaName: props.model.metadata.schema,
      databaseName: props.model.metadata.database,
      logicId: logic.id,
      lineageId: props.lineageId,
      targetOrganizationId: this.#targetOrganizationId,
      writeToPersistence: false,
    });

    const columns = await Promise.all(
      Object.keys(props.model.columns).map(async (columnKey) =>
        this.#generateColumn(
          props.model.columns[columnKey],
          mat.relationName,
          mat.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  #generateDbtSeedNode = async (props: {
    model: any;
    modelManifest: any;
    dbtDependentOn: MaterializationDefinition[];
    catalogFile: string;
    lineageId: string;
  }): Promise<void> => {
    const mat = await this.#generateNodeMaterialization({
      materializationType: props.model.metadata.type,
      name: props.model.metadata.name,
      relationName: props.modelManifest.relation_name,
      schemaName: props.model.metadata.schema,
      databaseName: props.model.metadata.database,
      lineageId: props.lineageId,
      targetOrganizationId: this.#targetOrganizationId,
      writeToPersistence: false,
    });

    const columns = await Promise.all(
      Object.keys(props.model.columns).map(async (columnKey) =>
        this.#generateColumn(
          props.model.columns[columnKey],
          mat.relationName,
          mat.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  /* Generate resources that are either defined in catalog and manifest.json 
  or are only reference by SQL models (e.g. sf source table that are not defined as dbt sources) */
  #generateDbtNode = async (props: {
    model: any;
    modelManifest: any;
    dbtDependentOn: MaterializationDefinition[];
    catalogFile: string;
  }): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    switch (props.modelManifest.resource_type) {
      case 'model':
        await this.#generateDbtModelNode({ ...props, lineageId: lineage.id });
        break;
      case 'seed':
        await this.#generateDbtSeedNode({ ...props, lineageId: lineage.id });
        break;
      case 'test':
        break;
      default:
        throw new Error(
          'Unhandled dbt node type detected while generating dbt node resources'
        );
    }
  };

  /* Get dbt nodes from catalog.json or manifest.json */
  #getDbtResources = (file: string): DbtResources => {
    const data = file;

    const content = JSON.parse(data);

    const { nodes } = content;
    const { sources } = content;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const { parent_map } = content;

    return { nodes, sources, parent_map };
  };

  /* Runs through dbt nodes and creates objects like logic, materializations and columns */
  #generateWarehouseResources = async (
    catalog: any,
    manifest: any
  ): Promise<void> => {
    const uniqueIdRelationNameMapping: {
      [key: string]: { relationName: string };
    } = {};

    const dbtCatalogResources = this.#getDbtResources(catalog);
    const dbtManifestResources = this.#getDbtResources(manifest);

    const dbtCatalogSourceKeys = Object.keys(dbtCatalogResources.sources);
    const dbtManifestSourceKeys = Object.keys(dbtManifestResources.sources);

    dbtManifestSourceKeys.forEach((key: string) => {
      uniqueIdRelationNameMapping[key] = {
        relationName: dbtManifestResources.sources[key].relation_name,
      };
    });

    dbtCatalogSourceKeys.forEach((key) => {
      const source = dbtCatalogResources.sources[key];

      const matCatalogElement = {
        relationName: uniqueIdRelationNameMapping[key].relationName,
        materializationName: source.metadata.name,
        schemaName: source.metadata.schema,
        databaseName: source.metadata.database,
      };

      this.#matDefinitionCatalog.push(matCatalogElement);
    });

    await Promise.all(
      dbtCatalogSourceKeys.map(async (key) =>
        this.#generateWarehouseSource(dbtCatalogResources.sources[key])
      )
    );

    const dbtCatalogNodeKeys = Object.keys(dbtCatalogResources.nodes);

    if (!dbtCatalogNodeKeys.length)
      throw new ReferenceError('No dbt models found');

    const dbtManifestNodeKeys = Object.keys(dbtManifestResources.nodes);

    dbtManifestNodeKeys.forEach((key: string) => {
      uniqueIdRelationNameMapping[key] = {
        relationName: dbtManifestResources.nodes[key].relation_name,
      };
    });

    dbtCatalogNodeKeys.forEach((key) => {
      const model = dbtCatalogResources.nodes[key];

      const matCatalogElement = {
        relationName: uniqueIdRelationNameMapping[key].relationName,
        materializationName: model.metadata.name,
        schemaName: model.metadata.schema,
        databaseName: model.metadata.database,
      };

      this.#matDefinitionCatalog.push(matCatalogElement);
    });

    await Promise.all(
      dbtCatalogNodeKeys.map(async (key) => {
        const dependsOn: string[] = [
          ...new Set(dbtManifestResources.parent_map[key]),
        ];

        const dependsOnRelationName = dependsOn.map(
          (dependencyKey) =>
            uniqueIdRelationNameMapping[dependencyKey].relationName
        );

        const dbtDependentOn = this.#matDefinitionCatalog.filter((element) =>
          dependsOnRelationName.includes(element.relationName)
        );

        if (dependsOnRelationName.length !== dbtDependentOn.length)
          throw new RangeError('materialization dependency mismatch');

        return this.#generateDbtNode({
          model: dbtCatalogResources.nodes[key],
          modelManifest: dbtManifestResources.nodes[key],
          dbtDependentOn,
          catalogFile: catalog,
        });
      })
    );
  };

  // /* Identifies the statement root (e.g. create_materialization_statement.select_statement) of a specific reference path */
  // #getStatementRoot = (path: string): string => {
  //   const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
  //   if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
  //     // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
  //     throw new RangeError('Statement root not found for column reference');

  //   return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  // };

  /* Checks if parent dependency can be mapped on the provided self column or to another column of the self materialization. */
  // #isDependencyOfTarget = (
  //   potentialDependency: ColumnRef,
  //   selfRef: ColumnRef
  // ): boolean => {
  //   const dependencyStatementRoot = this.#getStatementRoot(
  //     potentialDependency.context.path
  //   );
  //   const selfStatementRoot = this.#getStatementRoot(selfRef.context.path);

  //   const isStatementDependency =
  //     !potentialDependency.context.path.includes(SQLElement.INSERT_STATEMENT) &&
  //     !potentialDependency.context.path.includes(
  //       SQLElement.COLUMN_DEFINITION
  //     ) &&
  //     dependencyStatementRoot === selfStatementRoot &&
  //     (potentialDependency.context.path.includes(SQLElement.COLUMN_REFERENCE) ||
  //       potentialDependency.context.path.includes(
  //         SQLElement.WILDCARD_IDENTIFIER
  //       ));

  //   if (!isStatementDependency) return false;

  //   const isSelfSelectStatement = selfStatementRoot.includes(
  //     SQLElement.SELECT_STATEMENT
  //   );

  //   const isWildcardRef =
  //     isSelfSelectStatement && potentialDependency.isWildcardRef;
  //   const isSameName =
  //     isSelfSelectStatement && selfRef.name === potentialDependency.name;
  //   const isGroupBy =
  //     potentialDependency.context.path.includes(SQLElement.GROUPBY_CLAUSE) &&
  //     selfRef.name !== potentialDependency.name;
  //   const isJoinOn =
  //     potentialDependency.context.path.includes(SQLElement.JOIN_ON_CONDITION) &&
  //     selfRef.name !== potentialDependency.name;

  //   if (isWildcardRef || isSameName || isGroupBy) return true;

  //   if (isJoinOn) return false;
  //   if (potentialDependency.name !== selfRef.name) return false;

  //   throw new RangeError(
  //     'Unhandled case when checking if is dependency of target'
  //   );
  // };

  #retrieveQueryHistory = async (biLayer: BiLayer): Promise<any> => {
    const queryHistoryResult: QueryHistoryResponseDto =
      await this.#querySnowflakeHistory.execute(
        {
          biLayer,
          limit: 10,
          targetOrganizationId: this.#targetOrganizationId,
        },
        { jwt: this.#jwt, callerOrganizationId: this.#callerOrganizationId }
      );

    if (!queryHistoryResult.success) throw new Error(queryHistoryResult.error);
    if (!queryHistoryResult.value)
      throw new SyntaxError(`Retrival of query history failed`);

    return queryHistoryResult.value;
  };

  /* Get all relevant dashboards that are data dependency to self materialization */
  #getDashboardDataDependencyRefs = async (
    statementRefs: Refs,
    queryHistory: any,
    biLayer: BiLayer
  ): Promise<DashboardRef[]> => {
    const dependentDashboards: DashboardRef[] = [];

    statementRefs.columns.forEach((column) => {
      queryHistory[Object.keys(queryHistory)[0]].forEach((entry: any) => {
        const sqlText: string = entry.QUERY_TEXT;

        const testUrl = sqlText.match(/"(https?:[^\s]+),/);
        const dashboardUrl = testUrl
          ? testUrl[1]
          : `${biLayer} dashboard: ${new ObjectId().toHexString()}`;

        const matName = column.materializationName.toUpperCase();
        const colName = column.alias
          ? column.alias.toUpperCase()
          : column.name.toUpperCase();

        if (sqlText.includes(matName) && sqlText.includes(colName)) {
          dependentDashboards.push({
            url: dashboardUrl,
            materializationName: matName,
            columnName: colName,
          });
        }
      });
    });
    return dependentDashboards;
  };

  /* Get all relevant wildcard statement references that are data dependency to self materialization */
  #getWildcardDataDependencyRefs = (statementRefs: Refs): ColumnRef[] =>
    statementRefs.wildcards.filter(
      (wildcard) => wildcard.dependencyType === DependencyType.DATA
    );

  /* Get all relevant column statement references that are data dependency to self materialization */
  #getColDataDependencyRefs = (statementRefs: Refs): ColumnRef[] => {
    let dataDependencyRefs = statementRefs.columns.filter(
      (column) =>
        column.dependencyType === DependencyType.DATA &&
        !column.isCompoundValueRef
    );

    const setColumnRefs = dataDependencyRefs.filter((ref) =>
      ref.context.path.includes(SQLElement.SET_EXPRESSION)
    );

    const uniqueSetColumnRefs = setColumnRefs.filter(
      (value, index, self) =>
        index ===
        self.findIndex(
          (ref) =>
            this.#insensitiveEquality(ref.name, value.name) &&
            this.#insensitiveEquality(ref.context.path, value.context.path) &&
            this.#insensitiveEquality(
              ref.materializationName,
              value.materializationName
            )
        )
    );

    const columnRefs = dataDependencyRefs.filter(
      (ref) => !ref.context.path.includes(SQLElement.SET_EXPRESSION)
    );

    dataDependencyRefs = uniqueSetColumnRefs.concat(columnRefs);

    // const withColumnRefs = dataDependencyRefs.filter(
    //   (ref) =>
    //     ref.context.path.includes(SQLElement.WITH_COMPOUND_STATEMENT) &&
    //     !ref.context.path.includes(SQLElement.COMMON_TABLE_EXPRESSION)
    // );
    // columnRefs = dataDependencyRefs.filter(
    //   (ref) => !ref.context.path.includes(SQLElement.WITH_COMPOUND_STATEMENT)
    // );

    // dataDependencyRefs = withColumnRefs.concat(columnRefs);

    return dataDependencyRefs;
  };

  #buildDashboardRefDependency = async (
    dashboardRef: DashboardRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const lineage = this.#lineage;
    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const lineageId = lineage.id;
    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const materialization = await this.#materializationRepo.findBy(
      {
        name: dashboardRef.materializationName,
        relationName: parentRelationNames[0],
        lineageId,
        organizationId: this.#organizationId,
      },
      this.#dbConnection
    );
    const materializationId = materialization[0].id;

    const column = await this.#columnRepo.findBy(
      {
        name: dashboardRef.columnName,
        materializationId,
        lineageId,
        organizationId: this.#organizationId,
      },
      this.#dbConnection
    );
    const columnId = column[0].id;

    const createDashboardResult = await this.#createDashboard.execute(
      {
        columnId,
        columnName: dashboardRef.columnName,
        lineageId,
        materializationId,
        materializationName: dashboardRef.materializationName,
        url: dashboardRef.url,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createDashboardResult.success)
      throw new Error(createDashboardResult.error);
    if (!createDashboardResult.value)
      throw new Error('Creating dashboard failed');

    const dashboard = createDashboardResult.value;

    this.#dashboards.push(dashboard);

    const createExternalDependencyResult =
      await this.#createExternalDependency.execute(
        {
          dashboard,
          lineageId: lineage.id,
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        {
          isSystemInternal: this.#isSystemInternal,
          callerOrganizationId: this.#callerOrganizationId,
        },
        this.#dbConnection
      );

    if (!createExternalDependencyResult.success)
      throw new Error(createExternalDependencyResult.error);
    if (!createExternalDependencyResult.value)
      throw new ReferenceError(`Creating external dependency failed`);

    const dependency = createExternalDependencyResult.value;
    this.#dependencies.push(dependency);
  };

  /* Creates dependency for specific wildcard ref */
  #buildWildcardRefDependency = async (
    dependencyRef: ColumnRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const lineage = this.#lineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const columnDependencyRefs = await this.#getDependenciesForWildcard(
      dependencyRef
    );

    // const isCreateDependencyResponse = (
    //   item: CreateDependencyResponse | null
    // ): item is CreateDependencyResponse => !!item;

    const createDependencyResults = await Promise.all(
      columnDependencyRefs.map(
        async (dependency): Promise<CreateDependencyResponse> => {
          // if (this.#columnRefIsEqual(dependency, this.#lastQueryDependency))
          //   return null;

          // if (dependency.dependencyType === DependencyType.QUERY)
          //   this.#lastQueryDependency = dependency;

          const createDependencyResult = await this.#createDependency.execute(
            {
              dependencyRef: dependency,
              selfRelationName: relationName,
              parentRelationNames,
              lineageId: lineage.id,
              targetOrganizationId: this.#targetOrganizationId,
              writeToPersistence: false,
            },
            {
              isSystemInternal: this.#isSystemInternal,
              callerOrganizationId: this.#callerOrganizationId,
            },
            this.#dbConnection
          );

          return createDependencyResult;
        }
      )
    );

    // const onlyCreateDependencyResults = createDependencyResults.filter(
    //   isCreateDependencyResponse
    // );

    if (createDependencyResults.some((result) => !result.success)) {
      const errorResults = createDependencyResults.filter(
        (result) => result.error
      );
      throw new Error(errorResults[0].error);
    }

    if (createDependencyResults.some((result) => !result.value))
      console.warn(`Fix. Creation of dependencies failed. Skipped for now.`);
    // throw new SyntaxError(`Creation of dependencies failed`);

    const isValue = (item: Dependency | undefined): item is Dependency =>
      !!item;

    const values = createDependencyResults
      .map((result) => result.value)
      .filter(isValue);

    this.#dependencies.push(...values);
  };

  /* Creates dependency for specific column ref */
  #buildColumnRefDependency = async (
    dependencyRef: ColumnRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const lineage = this.#lineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const createDependencyResult = await this.#createDependency.execute(
      {
        dependencyRef,
        selfRelationName: relationName,
        parentRelationNames,
        lineageId: lineage.id,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createDependencyResult.success)
      throw new Error(createDependencyResult.error);
    if (!createDependencyResult.value) {
      console.warn(`Creating dependency failed`);
      return;
    }
    // throw new ReferenceError(`Creating dependency failed`);

    const dependency = createDependencyResult.value;

    this.#dependencies.push(dependency);
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildDependencies = async (biType?: string): Promise<void> => {
    // todo - should method be completely sync? Probably resolves once transformed into batch job.

    let biLayer: BiLayer | undefined;
    let queryHistory: any | undefined;

    if (biType) {
      biLayer = parseBiLayer(biType);
      queryHistory = await this.#retrieveQueryHistory(biLayer);
    }

    await Promise.all(
      this.#logics.map(async (logic) => {
        const colDataDependencyRefs = this.#getColDataDependencyRefs(
          logic.statementRefs
        );
        await Promise.all(
          colDataDependencyRefs.map(async (dependencyRef) =>
            this.#buildColumnRefDependency(
              dependencyRef,
              logic.relationName,
              logic.dependentOn.dbtDependencyDefinitions
                .concat(logic.dependentOn.dwDependencyDefinitions)
                .map((element) => element.relationName)
            )
          )
        );

        const wildcardDataDependencyRefs = this.#getWildcardDataDependencyRefs(
          logic.statementRefs
        );

        await Promise.all(
          wildcardDataDependencyRefs.map(async (dependencyRef) =>
            this.#buildWildcardRefDependency(
              dependencyRef,
              logic.relationName,
              logic.dependentOn.dbtDependencyDefinitions
                .concat(logic.dependentOn.dwDependencyDefinitions)
                .map((element) => element.relationName)
            )
          )
        );

        if (biLayer && queryHistory) {
          const dashboardDataDependencyRefs =
            await this.#getDashboardDataDependencyRefs(
              logic.statementRefs,
              queryHistory,
              biLayer
            );

          const uniqueDashboardRefs = dashboardDataDependencyRefs.filter(
            (value, index, self) =>
              index ===
              self.findIndex((dashboard) =>
                typeof dashboard.name === 'string' &&
                typeof value.name === 'string'
                  ? this.#insensitiveEquality(dashboard.name, value.name)
                  : dashboard.name === value.name &&
                    this.#insensitiveEquality(
                      dashboard.columnName,
                      value.columnName
                    ) &&
                    this.#insensitiveEquality(
                      dashboard.materializationName,
                      value.materializationName
                    )
              )
          );

          await Promise.all(
            uniqueDashboardRefs.map(async (dashboardRef) =>
              this.#buildDashboardRefDependency(
                dashboardRef,
                logic.relationName,
                logic.dependentOn.dbtDependencyDefinitions.map(
                  (element) => element.relationName
                )
              )
            )
          );
        }
      })
    );
  };

  #getDependenciesForWildcard = async (
    dependencyRef: ColumnRef
  ): Promise<ColumnRef[]> => {
    const lineage = this.#lineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const catalogMatches = this.#matDefinitionCatalog.filter((dependency) => {
      const nameIsEqual = this.#insensitiveEquality(
        dependencyRef.materializationName,
        dependency.materializationName
      );

      const schemaNameIsEqual =
        !dependencyRef.schemaName ||
        (typeof dependencyRef.schemaName === 'string' &&
        typeof dependency.schemaName === 'string'
          ? this.#insensitiveEquality(
              dependencyRef.schemaName,
              dependency.schemaName
            )
          : dependencyRef.schemaName === dependency.schemaName);

      const databaseNameIsEqual =
        !dependencyRef.databaseName ||
        (typeof dependencyRef.databaseName === 'string' &&
        typeof dependency.databaseName === 'string'
          ? this.#insensitiveEquality(
              dependencyRef.databaseName,
              dependency.databaseName
            )
          : dependencyRef.databaseName === dependency.databaseName);

      return nameIsEqual && schemaNameIsEqual && databaseNameIsEqual;
    });

    if (catalogMatches.length !== 1) {
      console.warn(
        'todo - fix. Error in wildcard dependency generation. Skipped for now'
      );
      return [];
      //   throw new RangeError(
      //   'Inconsistencies in materialization dependency catalog'
      // );
    }

    const { relationName } = catalogMatches[0];

    const readColumnsResult = await this.#readColumns.execute(
      {
        relationName,
        lineageId: lineage.id,
        targetOrganizationId: this.#targetOrganizationId,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of columns failed`);

    const colsFromWildcard = readColumnsResult.value;

    const dependencies = colsFromWildcard.map((column) => ({
      ...dependencyRef,
      name: column.name,
    }));

    return dependencies;
  };

  #writeWhResourcesToPersistence = async (
    lineageId?: string,
    lineageCreatedAt?: string
  ): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError(
        'Lineage object does not exist. Cannot write to persistence'
      );
    if (!(lineageId && lineageCreatedAt))
      await this.#lineageRepo.insertOne(this.#lineage, this.#dbConnection);

    await this.#logicRepo.insertMany(this.#logics, this.#dbConnection);

    await this.#materializationRepo.insertMany(
      this.#materializations,
      this.#dbConnection
    );

    await this.#columnRepo.insertMany(this.#columns, this.#dbConnection);
  };

  #writeDashboardsToPersistence = async (): Promise<void> => {
    if (this.#dashboards.length > 0)
      await this.#dashboardRepo.insertMany(
        this.#dashboards,
        this.#dbConnection
      );
  };

  #writeDependenciesToPersistence = async (): Promise<void> => {
    if (this.#dependencies.length > 0)
      await this.#dependencyRepo.insertMany(
        this.#dependencies,
        this.#dbConnection
      );
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateLineageResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#dbConnection = dbConnection;

      this.#targetOrganizationId = request.targetOrganizationId;

      this.#callerOrganizationId = auth.callerOrganizationId;

      if (auth.callerOrganizationId)
        this.#organizationId = auth.callerOrganizationId;
      else if (request.targetOrganizationId)
        this.#organizationId = request.targetOrganizationId;
      else throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#jwt = auth.jwt;
      this.#isSystemInternal = auth.isSystemInternal;

      // todo - Workaround. Fix ioc container
      this.#lineage = undefined;
      this.#logics = [];
      this.#materializations = [];
      this.#columns = [];
      this.#dependencies = [];
      this.#matDefinitionCatalog = [];
      this.#lastQueryDependency = undefined;

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      await this.#buildLineage();

      console.log('...generating warehouse resources');
      await this.#generateWarehouseResources(request.catalog, request.manifest);

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence(
        request.lineageId,
        request.lineageCreatedAt
      );

      console.log('...building dependencies');
      await this.#buildDependencies(request.biType);

      console.log('...writing dashboards to persistence');
      await this.#writeDashboardsToPersistence();

      console.log('...writing dependencies to persistence');
      await this.#writeDependenciesToPersistence();

      // todo - how to avoid checking if property exists. A sub-method created the property
      if (!this.#lineage)
        throw new ReferenceError('Lineage property is undefined');

      console.log('finished lineage creation.');

      return Result.ok(this.#lineage);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #insensitiveEquality = (str1: string, str2: string): boolean =>
    str1.toLowerCase() === str2.toLowerCase();
}
