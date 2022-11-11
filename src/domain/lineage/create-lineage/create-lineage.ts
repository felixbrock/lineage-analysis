// todo - Clean Architecture dependency violation. Fix
import Result from '../../value-types/transient-types/result';
import IUseCase from '../../services/use-case';
import { CreateColumn } from '../../column/create-column';
import { CreateMaterialization } from '../../materialization/create-materialization';
import { CreateLogic } from '../../logic/create-logic';
import { ParseSQL } from '../../sql-parser-api/parse-sql';
import { Lineage } from '../../entities/lineage';
import { Logic } from '../../entities/logic';
import { CreateDependency } from '../../dependency/create-dependency';
import { Dependency } from '../../entities/dependency';
import { ReadColumns } from '../../column/read-columns';
import { Materialization } from '../../entities/materialization';
import { Column } from '../../entities/column';
import { ILineageRepo, LineageUpdateDto } from '../i-lineage-repo';
import { IDependencyRepo } from '../../dependency/i-dependency-repo';
import {} from '../../services/i-db';
import { QuerySfQueryHistory } from '../../snowflake-api/query-snowflake-history';
import { Dashboard } from '../../entities/dashboard';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { IDashboardRepo } from '../../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import { buildLineage } from './build-lineage';
import { DbtDataEnvGenerator } from './dbt-data-env-generator';
import { BiType } from '../../value-types/bilayer';
import { SfDataEnvGenerator } from './sf-data-env-generator';
import { ILogicRepo } from '../../logic/i-logic-repo';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { IColumnRepo } from '../../column/i-column-repo';
import { QuerySnowflake } from '../../snowflake-api/query-snowflake';
import DataEnvMerger from './data-env-merger';
import DependenciesBuilder from './dependencies-builder';

export interface CreateLineageRequestDto {
  targetOrganizationId?: string;
  dbtCatalog?: string;
  dbtManifest?: string;
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
      CreateLineageAuthDto
    >
{
  readonly #createLogic: CreateLogic;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createDashboard: CreateDashboard;

  readonly #createDependency: CreateDependency;

  readonly #createExternalDependency: CreateExternalDependency;

  readonly #parseSQL: ParseSQL;

  readonly #querySnowflake: QuerySnowflake;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  readonly #lineageRepo: ILineageRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  readonly #dashboardRepo: IDashboardRepo;

  readonly #readColumns: ReadColumns;

  #targetOrgId?: string;

  #auth?: CreateLineageAuthDto;

  constructor(
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    createExternalDependency: CreateExternalDependency,
    parseSQL: ParseSQL,
    lineageRepo: ILineageRepo,
    logicRepo: ILogicRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    dependencyRepo: IDependencyRepo,
    dashboardRepo: IDashboardRepo,
    readColumns: ReadColumns,
    createDashboard: CreateDashboard,
    querySnowflake: QuerySnowflake,
    querySfQueryHistory: QuerySfQueryHistory
  ) {
    this.#createLogic = createLogic;
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#createExternalDependency = createExternalDependency;
    this.#createDashboard = createDashboard;
    this.#parseSQL = parseSQL;
    this.#lineageRepo = lineageRepo;
    this.#logicRepo = logicRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
    this.#dashboardRepo = dashboardRepo;
    this.#readColumns = readColumns;
    this.#querySnowflake = querySnowflake;
    this.#querySfQueryHistory = querySfQueryHistory;
  }

  #writeWhResourcesToPersistence = async (props: {
    lineage: Lineage;
    matsToCreate: Materialization[];
    matsToReplace: Materialization[];
    columnsToCreate: Column[];
    columnsToReplace: Column[];
    logicsToCreate: Logic[];
    logicsToReplace: Logic[];
  }): Promise<void> => {
    if (!this.#auth) throw new Error('auth obj not avaible');

    await this.#lineageRepo.insertOne(
      props.lineage,
      this.#auth,
      this.#targetOrgId
    );

    if (props.logicsToReplace.length)
      await this.#logicRepo.replaceMany(
        props.logicsToReplace,
        this.#auth,
        this.#targetOrgId
      );
    if (props.matsToReplace.length)
      await this.#materializationRepo.replaceMany(
        props.matsToReplace,
        this.#auth,
        this.#targetOrgId
      );
    if (props.columnsToReplace.length)
      await this.#columnRepo.replaceMany(
        props.columnsToReplace,
        this.#auth,
        this.#targetOrgId
      );

    if (props.logicsToCreate.length)
      await this.#logicRepo.insertMany(
        props.logicsToCreate,
        this.#auth,
        this.#targetOrgId
      );

    if (props.matsToCreate.length)
      await this.#materializationRepo.insertMany(
        props.matsToCreate,
        this.#auth,
        this.#targetOrgId
      );

    if (props.columnsToCreate.length)
      await this.#columnRepo.insertMany(
        props.columnsToCreate,
        this.#auth,
        this.#targetOrgId
      );
  };

  #writeDashboardsToPersistence = async (
    dashboards: Dashboard[]
  ): Promise<void> => {
    if (!this.#auth) throw new Error('auth obj not avaible');

    if (dashboards.length)
      await this.#dashboardRepo.insertMany(
        dashboards,
        this.#auth,
        this.#targetOrgId
      );
  };

  #writeDependenciesToPersistence = async (
    dependencies: Dependency[]
  ): Promise<void> => {
    if (!this.#auth) throw new Error('auth obj not avaible');

    if (dependencies.length)
      await this.#dependencyRepo.insertMany(
        dependencies,
        this.#auth,
        this.#targetOrgId
      );
  };

  #updateLineage = async (
    id: string,
    updateDto: LineageUpdateDto
  ): Promise<void> => {
    if (!this.#auth) throw new Error('auth obj not avaible');

    await this.#lineageRepo.updateOne(
      id,
      updateDto,
      this.#auth,
      this.#targetOrgId
    );
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
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

      let orgId: string;
      if (auth.callerOrganizationId) orgId = auth.callerOrganizationId;
      else if (request.targetOrganizationId)
        orgId = request.targetOrganizationId;
      else throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#auth = auth;
      this.#targetOrgId = request.targetOrganizationId;

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      const lineage = buildLineage();

      if (!!request.dbtCatalog !== !!request.dbtManifest)
        throw new Error(
          'When creating lineage based on dbt both, the manifest and catalog file have to be provided'
        );

      const { dbtCatalog, dbtManifest } = request;

      const dbtBased = dbtCatalog && dbtManifest;

      console.log('...generating warehouse resources');
      const { jwt, ...remainingAuth } = auth;
      const dataEnvGenerator = dbtBased
        ? new DbtDataEnvGenerator(
            {
              dbtCatalog,
              dbtManifest,
              lineageId: lineage.id,
              targetOrganizationId: request.targetOrganizationId,
            },
            remainingAuth,
            {
              createColumn: this.#createColumn,
              createLogic: this.#createLogic,
              createMaterialization: this.#createMaterialization,
              parseSQL: this.#parseSQL,
            }
          )
        : new SfDataEnvGenerator(
            {
              lineageId: lineage.id,
              targetOrganizationId: request.targetOrganizationId,
            },
            auth,
            {
              createColumn: this.#createColumn,
              createMaterialization: this.#createMaterialization,
              createLogic: this.#createLogic,
              parseSQL: this.#parseSQL,
              querySnowflake: this.#querySnowflake,
            }
          );
      const { materializations, columns, logics, catalog } =
        await dataEnvGenerator.generate();

      console.log('...merging new lineage snapshot with last one');
      const dataEnvMerger = new DataEnvMerger(
        { columns, materializations, logics, organizationId: orgId },
        {
          lineageRepo: this.#lineageRepo,
          columnRepo: this.#columnRepo,
          logicRepo: this.#logicRepo,
          materializationRepo: this.#materializationRepo,
        }
      );

      const mergedDataEnv = await dataEnvMerger.merge();

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence({ lineage, ...mergedDataEnv });

      console.log('...building dependencies');
      const dependenciesBuilder = await new DependenciesBuilder(
        {
          lineageId: lineage.id,
          logics: mergedDataEnv.logicsToCreate.concat(
            mergedDataEnv.logicsToReplace
          ),
          mats: mergedDataEnv.matsToCreate.concat(mergedDataEnv.matsToReplace),
          columns: mergedDataEnv.columnsToCreate.concat(
            mergedDataEnv.columnsToReplace
          ),
          catalog,
          organizationId: orgId,
          targetOrganizationId: request.targetOrganizationId,
        },
        auth,
        {
          createDashboard: this.#createDashboard,
          createDependency: this.#createDependency,
          createExternalDependency: this.#createExternalDependency,
          readColumns: this.#readColumns,
          querySfQueryHistory: this.#querySfQueryHistory,
        }
      );
      const { dashboards, dependencies } = await dependenciesBuilder.build(
        request.biType
      );

      console.log('...writing dashboards to persistence');
      await this.#writeDashboardsToPersistence(dashboards);

      console.log('...writing dependencies to persistence');
      await this.#writeDependenciesToPersistence(dependencies);

      console.log('...setting lineage complete state to true');
      await this.#updateLineage(lineage.id, { completed: true });

      console.log('finished lineage creation.');

      return Result.ok(
        Lineage.build({
          id: lineage.id,
          createdAt: lineage.createdAt,
          completed: true,
        })
      );
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
