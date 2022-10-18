// todo - Clean Architecture dependency violation. Fix
import Result from '../../value-types/transient-types/result';
import IUseCase from '../../services/use-case';
import { CreateColumn } from '../../column/create-column';
import { CreateMaterialization } from '../../materialization/create-materialization';
import { CreateLogic } from '../../logic/create-logic';
import { ParseSQL } from '../../sql-parser-api/parse-sql';
import { Lineage } from '../../entities/lineage';
import {
  Logic,
  ColumnRef,
  MaterializationDefinition,
} from '../../entities/logic';
import { CreateDependency } from '../../dependency/create-dependency';
import { Dependency } from '../../entities/dependency';
import { ReadColumns } from '../../column/read-columns';
import { Materialization } from '../../entities/materialization';
import { Column } from '../../entities/column';
import { ILineageRepo } from '../i-lineage-repo';
import { IColumnRepo } from '../../column/i-column-repo';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { IDependencyRepo } from '../../dependency/i-dependency-repo';
import { ILogicRepo } from '../../logic/i-logic-repo';
import { DbConnection } from '../../services/i-db';
import { QuerySnowflakeHistory } from '../../query-snowflake-history-api/query-snowflake-history';
import { Dashboard } from '../../entities/dashboard';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { IDashboardRepo } from '../../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import { buildLineage } from './build-lineage';
import { DataEnvGenerator } from './data-env-generator';
import DependenciesBuilder from './dependencies-builder';
import { BiType } from '../../value-types/bilayer';

export interface CreateLineageRequestDto {
  targetOrganizationId?: string;
  catalog: string;
  manifest: string;
  biType?: BiType;
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

  #writeDashboardsToPersistence = async (dashboards: Dashboard[]): Promise<void> => {
    if (this.#newDashboards.length > 0)
      await this.#dashboardRepo.insertMany(
        dashboards,
        this.#dbConnection
      );
  };

  #writeDependenciesToPersistence = async (dependencies: Dependency[]): Promise<void> => {
    if (this.#newDependencies.length > 0)
      await this.#dependencyRepo.insertMany(
        dependencies,
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

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      const lineage = buildLineage(this.#organizationId);
      this.#newLineage = lineage;

      console.log('...generating warehouse resources');
      const { jwt, ...remainingAuth } = auth;
      const dataEnvGenerator = new DataEnvGenerator(
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
      const { materializations, columns, logics, matDefinitions } =
        await dataEnvGenerator.generate();

      console.log('...merging new lineage snapshot with last one');
      await this.#mergeWithLatestSnapshot();

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence();

      console.log('...building dependencies');
      const dependenciesBuilder = await new DependenciesBuilder(
        {
          lineageId: lineage.id,
          logics,
          matDefinitions,
          organizationId: this.#organizationId,
          targetOrganizationId: this.#targetOrganizationId,
        },
        auth,
        dbConnection,
        {
          createDashboard: this.#createDashboard,
          createDependency: this.#createDependency,
          createExternalDependency: this.#createExternalDependency,
          readColumns: this.#readColumns,
          querySnowflakeHistory: this.#querySnowflakeHistory,
          columnRepo: this.#columnRepo,
          materializationRepo: this.#materializationRepo,
        }
      );
      const {dashboards, dependencies} = await dependenciesBuilder.build(request.biType);

      // todo - merge dashboards and dependencies

      console.log('...writing dashboards to persistence');
      await this.#writeDashboardsToPersistence(dashboards);

      console.log('...writing dependencies to persistence');
      await this.#writeDependenciesToPersistence(dependencies);

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
