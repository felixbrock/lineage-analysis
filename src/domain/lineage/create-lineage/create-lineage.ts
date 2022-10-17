// todo - Clean Architecture dependency violation. Fix
import { ObjectId } from 'mongodb';
import Result from '../../value-types/transient-types/result';
import IUseCase from '../../services/use-case';
import SQLElement from '../../value-types/sql-element';
import { CreateColumn } from '../../column/create-column';
import {
  CreateMaterialization,
  CreateMaterializationRequestDto,
} from '../../materialization/create-materialization';
import { CreateLogic } from '../../logic/create-logic';
import { ParseSQL, ParseSQLResponseDto } from '../../sql-parser-api/parse-sql';
import { Lineage } from '../../entities/lineage';
import {
  Logic,
  ColumnRef,
  Refs,
  MaterializationDefinition,
  DashboardRef,
} from '../../entities/logic';
import {
  CreateDependency,
  CreateDependencyResponse,
} from '../../dependency/create-dependency';
import { Dependency, DependencyType } from '../../entities/dependency';
import { ReadColumns } from '../../column/read-columns';
import {
  Materialization,
  MaterializationType,
} from '../../entities/materialization';
import { Column } from '../../entities/column';
import { ILineageRepo } from '../i-lineage-repo';
import { IColumnRepo } from '../../column/i-column-repo';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { IDependencyRepo } from '../../dependency/i-dependency-repo';
import { ILogicRepo } from '../../logic/i-logic-repo';
import { DbConnection } from '../../services/i-db';
import {
  QuerySnowflakeHistory,
  QueryHistoryResponseDto,
} from '../../query-snowflake-history-api/query-snowflake-history';
import { Dashboard } from '../../entities/dashboard';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { IDashboardRepo } from '../../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import { BiLayer, parseBiLayer } from '../../value-types/bilayer';
import { buildLineage } from './build-lineage';
import { DataEnvResourcesGenerator } from './data-env-resources-generator';

export interface CreateLineageRequestDto {
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

  #newLineage?: Lineage;

  #newLogics: Logic[];

  #oldLogics: { [key: string]: Logic[] };

  #logicsToUpdate: Logic[];

  #logicsToCreate: Logic[];

  #newMaterializations: Materialization[];

  #matsToUpdate: Materialization[];

  #matsToCreate: Materialization[];

  #newColumns: Column[];

  #oldColumns: { [key: string]: Column[] };

  #columnsToUpdate: Column[];

  #columnsToCreate: Column[];

  #newDependencies: Dependency[];

  #oldDependencies: Dependency[];

  #depencenciesToUpdate: Dependency[];

  #depencenciesToCreate: Dependency[];

  #newDashboards: Dashboard[];

  #oldDashboards: { [key: string]: Dashboard[] };

  #dashboardsToUpdate: Dashboard[];

  #dashboardsToCreate: Dashboard[];

  #lastQueryDependency?: ColumnRef;

  #newMatDefinitionCatalog: MaterializationDefinition[];

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
  }

  #updateMatRelatedResources = (props: {
    oldMatProps: { matId: string; relationName: string; lineageId: string };
    newMat: Materialization;
  }) => {
    const createMatResult = await this.#createMaterialization.execute(
      {
        ...props.newMat.toDto(),
        id: props.oldMatProps.matId,
        relationName: props.oldMatProps.relationName,
        writeToPersistence: false,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createMatResult.success) throw new Error(createMatResult.error);
    if (!createMatResult.value)
      throw new Error('Create Mat failed - Unknown error');

    this.#matsToUpdate.push(createMatResult.value);
  };

  #groupByMatId = <T extends { materializationId: string }>(
    accumulation: { [key: string]: T[] },
    element: T
  ): { [key: string]: T[] } => {
    const localAcc = accumulation;

    const key = element.materializationId;
    if (!(key in accumulation)) {
      localAcc[key] = [];
    }
    localAcc[key].push(element);
    return localAcc;
  };

  #mergeWithLatestSnapshot = async (): Promise<void> => {
    // todo- needs to retrieve latest compeleted lineage
    const latestLineage = await this.#lineageRepo.findLatest(
      this.#dbConnection,
      this.#organizationId
    );

    if (!latestLineage) return;

    const latestMats = await this.#materializationRepo.findBy(
      { lineageIds: [latestLineage.id], organizationId: this.#organizationId },
      this.#dbConnection
    );

    await Promise.all(
      this.#newMaterializations.map(async (newMat) => {
        const matchingMat = latestMats.find(
          (oldMat) => newMat.relationName === oldMat.relationName
        );

        if (matchingMat) {
          if (!Object.keys(this.#oldColumns).length)
            this.#oldColumns = (
              await this.#columnRepo.findBy(
                {
                  lineageIds: [latestLineage.id],
                  organizationId: this.#organizationId,
                },
                this.#dbConnection
              )
            ).reduce(this.#groupByMatId, {});

          if (!Object.keys(this.#oldLogics).length)
            this.#oldColumns = (
              await this.#columnRepo.findBy(
                {
                  lineageIds: [latestLineage.id],
                  organizationId: this.#organizationId,
                },
                this.#dbConnection
              )
            ).reduce(this.#groupByMatId, {});

          await this.#updateMatRelatedResources;
        } else {
          // todo - also logic, ...
          this.#matsToCreate.push(newMat);
        }
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



  

 

 

  #writeWhResourcesToPersistence = async (): Promise<void> => {
    if (!this.#newLineage)
      throw new ReferenceError(
        'Lineage object does not exist. Cannot write to persistence'
      );

    await this.#logicRepo.insertMany(this.#newLogics, this.#dbConnection);

    await this.#materializationRepo.insertMany(
      this.#newMaterializations,
      this.#dbConnection
    );

    await this.#columnRepo.insertMany(this.#newColumns, this.#dbConnection);
  };

  // todo - updateDashboards

  #writeDashboardsToPersistence = async (): Promise<void> => {
    if (this.#newDashboards.length > 0)
      await this.#dashboardRepo.insertMany(
        this.#newDashboards,
        this.#dbConnection
      );
  };

  #writeDependenciesToPersistence = async (): Promise<void> => {
    if (this.#newDependencies.length > 0)
      await this.#dependencyRepo.insertMany(
        this.#newDependencies,
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
      this.#newLineage = undefined;

      this.#newLogics = [];
      this.#logicsToUpdate = [];
      this.#logicsToCreate = [];

      this.#newMaterializations = [];
      this.#matsToUpdate = [];
      this.#matsToCreate = [];

      this.#newColumns = [];
      this.#columnsToUpdate = [];
      this.#columnsToCreate = [];

      this.#newDependencies = [];
      this.#depencenciesToUpdate = [];
      this.#depencenciesToCreate = [];

      this.#dashboardsToUpdate = [];
      this.#dashboardsToCreate = [];

      this.#newMatDefinitionCatalog = [];
      this.#lastQueryDependency = undefined;

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      const lineage = buildLineage(this.#organizationId);
      this.#newLineage = lineage;

      console.log('...generating warehouse resources');
      const { jwt, ...remainingAuth } = auth;
      const dataEnvResources = new DataEnvResourcesGenerator(
        {
          dbtCatalog: request.catalog,
          dbtManifest: request.manifest,
          lineageId: lineage.id,
          targetOrganizationId: request.targetOrganizationId,
        },
        remainingAuth,
        dbConnection,
        {
          createColumn: this.#createColumn,
          createLogic: this.#createLogic,
          createMaterialization: this.#createMaterialization,
          parseSQL: this.#parseSQL,
        }
      );
      const generateDataEnvResourcesResult = await dataEnvResources.generate();

      console.log('...merging new lineage snapshot with last one');
      await this.#mergeWithLatestSnapshot();

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence();

      console.log('...building dependencies');
      await this.#buildDependencies(request.biType);

      console.log('...writing dashboards to persistence');
      await this.#writeDashboardsToPersistence();

      console.log('...writing dependencies to persistence');
      await this.#writeDependenciesToPersistence();

      // todo - updateLineage;

      if (!this.#newLineage)
        throw new ReferenceError('Lineage property is undefined');

      console.log('finished lineage creation.');

      return Result.ok(this.#newLineage);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
