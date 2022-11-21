// todo - Clean Architecture dependency violation. Fix
import Result from '../../value-types/transient-types/result';
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
import { QuerySfQueryHistory } from '../../snowflake-api/query-snowflake-history';
import { Dashboard } from '../../entities/dashboard';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { IDashboardRepo } from '../../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import { buildLineage } from './build-lineage';
import { DbtDataEnvGenerator } from './dbt-data-env-generator';
import { BiTool } from '../../value-types/bi-tool';
import { SfDataEnvGenerator } from './sf-data-env-generator';
import { ILogicRepo } from '../../logic/i-logic-repo';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { IColumnRepo } from '../../column/i-column-repo';
import { QuerySnowflake } from '../../snowflake-api/query-snowflake';
import IUseCase from '../../services/use-case';
import { IConnectionPool } from '../../snowflake-api/i-snowflake-api-repo';
import BaseAuth from '../../services/base-auth';
import DataEnvMerger from './data-env-merger';
import DependenciesBuilder from './dependencies-builder';

export interface CreateLineageRequestDto {
  targetOrgId?: string;
  dbtCatalog?: string;
  dbtManifest?: string;
  biTool?: BiTool;
}

export type CreateLineageAuthDto = BaseAuth;

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

  #auth?: CreateLineageAuthDto;

  #connPool?: IConnectionPool;

  #req?: CreateLineageRequestDto;

  
  
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

  #writeLineageToPersistence = async (lineage: Lineage): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#lineageRepo.insertOne(
      lineage,
      this.#auth,
      this.#connPool,
      this.#req.targetOrgId
    );
  };

  #writeWhResourcesToPersistence = async (props: {
    matsToCreate: Materialization[];
    matsToReplace: Materialization[];
    columnsToCreate: Column[];
    columnsToReplace: Column[];
    logicsToCreate: Logic[];
    logicsToReplace: Logic[];
  }): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (props.logicsToReplace.length)
      await this.#logicRepo.replaceMany(
        props.logicsToReplace,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );
    if (props.matsToReplace.length)
      await this.#materializationRepo.replaceMany(
        props.matsToReplace,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );
    if (props.columnsToReplace.length)
      await this.#columnRepo.replaceMany(
        props.columnsToReplace,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );

    if (props.logicsToCreate.length)
      await this.#logicRepo.insertMany(
        props.logicsToCreate,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );

    if (props.matsToCreate.length)
      await this.#materializationRepo.insertMany(
        props.matsToCreate,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );

    if (props.columnsToCreate.length)
      await this.#columnRepo.insertMany(
        props.columnsToCreate,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );
  };

  #writeDashboardsToPersistence = async (
    dashboards: Dashboard[]
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dashboards.length)
      await this.#dashboardRepo.insertMany(
        dashboards,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );
  };

  #writeDependenciesToPersistence = async (
    dependencies: Dependency[]
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dependencies.length)
      await this.#dependencyRepo.insertMany(
        dependencies,
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );
  };

  #updateLineage = async (
    id: string,
    updateDto: LineageUpdateDto
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#lineageRepo.updateOne(
      id,
      updateDto,
      this.#auth,
      this.#connPool,
      this.#req.targetOrgId
    );
  };

#generateEnv = async (): Promise<unknown> => {
  if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

  console.log('...generating warehouse resources');

  let dataEnvGenerator: DbtDataEnvGenerator | SfDataEnvGenerator;
  if (this.#req.dbtCatalog && this.#req.dbtManifest)
    dataEnvGenerator = new DbtDataEnvGenerator(
      {
        dbtCatalog: this.#req.dbtCatalog,
        dbtManifest: this.#req.dbtManifest,
        lineageId: lineage.id,
        targetOrgId: this.#req.targetOrgId,
      },
      this.#auth,
      {
        createColumn: this.#createColumn,
        createLogic: this.#createLogic,
        createMaterialization: this.#createMaterialization,
        parseSQL: this.#parseSQL,
      }
    );
  else {
    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error(
        'Sf based lineage creation has to be invoked by user'
      );

    dataEnvGenerator = new SfDataEnvGenerator(
      {
        lineageId: lineage.id,
      },
      { ...this.#auth, callerOrgId },
      {
        createColumn: this.#createColumn,
        createMaterialization: this.#createMaterialization,
        createLogic: this.#createLogic,
        parseSQL: this.#parseSQL,
        querySnowflake: this.#querySnowflake,
      }
    );
  }
  const { materializations, columns, logics, catalog } =
    await dataEnvGenerator.generate(this.#connPool);
}

#refreshEnv = async () => {
  console.log('...merging new lineage snapshot with last one');
  const dataEnvMerger = new DataEnvMerger(
    { columns, materializations, logics },
    auth,
    {
      lineageRepo: this.#lineageRepo,
      columnRepo: this.#columnRepo,
      logicRepo: this.#logicRepo,
      materializationRepo: this.#materializationRepo,
    }
  );

  const mergedDataEnv = await dataEnvMerger.merge(connPool);

}

#getNewDataEnv = async (auth: BaseAuth, connPool: IConnectionPool): Promise<  {
  matsToCreate: Materialization[];
  matsToReplace: Materialization[];
  columnsToCreate: Column[];
  columnsToReplace: Column[];
  logicsToCreate: Logic[];
  logicsToReplace: Logic[];
}> =>{
  const latestLineage = await this.#lineageRepo.findLatest(
    { tolerateIncomplete: false },
    auth,
    connPool,
    this.#req.targetOrgId
  );

  if(!latestLineage)
  const results = this.#generateEnv()
  else
  const results = this.#refreshEnv()






};

  async execute(
    req: CreateLineageRequestDto,
    auth: CreateLineageAuthDto,
    connPool: IConnectionPool
  ): Promise<CreateLineageResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let orgId: string;
      if (auth.callerOrgId) orgId = auth.callerOrgId;
      else if (req.targetOrgId) orgId = req.targetOrgId;
      else throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#connPool = connPool;
      this.#auth = auth;
      


      console.log('starting lineage creation...');

      console.log('...building lineage object');
      const lineage = buildLineage();

      console.log('...writing lineage to persistence');
      await this.#writeLineageToPersistence(lineage);

      const { dbtCatalog, dbtManifest } = req;

      



      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence({ ...mergedDataEnv });

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
          targetOrgId: req.targetOrgId,
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
        connPool, req.biTool
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
