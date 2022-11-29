// todo - Clean Architecture dependency violation. Fix
import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import { Lineage } from '../entities/lineage';
import { Dependency } from '../entities/dependency';
import { ILineageRepo, LineageUpdateDto } from './i-lineage-repo';
import { IDependencyRepo } from '../dependency/i-dependency-repo';
import { Dashboard } from '../entities/dashboard';
import { IDashboardRepo } from '../dashboard/i-dashboard-repo';
import { BiToolType } from '../value-types/bi-tool';
import { ILogicRepo } from '../logic/i-logic-repo';
import { IMaterializationRepo } from '../materialization/i-materialization-repo';
import { IColumnRepo } from '../column/i-column-repo';
import IUseCase from '../services/use-case';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseAuth from '../services/base-auth';
import { DataEnv } from '../data-env/data-env';
import { GenerateDbtDataEnv } from '../data-env/generate-dbt-data-env';
import { GenerateSfDataEnv } from '../data-env/generate-sf-data-env';
import { UpdateSfDataEnv } from '../data-env/update-sf-data-env';
import { BuildDependencies, BuildResult } from '../dependency/build-dependencies';
import { ModelRepresentation } from '../entities/logic';

export interface CreateLineageRequestDto {
  targetOrgId?: string;
  dbtCatalog?: string;
  dbtManifest?: string;
  biTool?: BiToolType;
}

export type CreateLineageAuthDto = BaseAuth;

export type CreateLineageResponseDto = Result<Lineage>;

type DataEnvOperation = 'create' | 'update';

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto,
      IConnectionPool
    >
{
  readonly #lineageRepo: ILineageRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  readonly #dashboardRepo: IDashboardRepo;

  readonly #generateSfDataEnv: GenerateSfDataEnv;

  readonly #generateDbtDataEnv: GenerateDbtDataEnv;

  readonly #updateSfDataEnv: UpdateSfDataEnv;

  readonly #buildDependencies: BuildDependencies;

  #auth?: CreateLineageAuthDto;

  #connPool?: IConnectionPool;

  #req?: CreateLineageRequestDto;

  constructor(
    lineageRepo: ILineageRepo,
    logicRepo: ILogicRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    dependencyRepo: IDependencyRepo,
    dashboardRepo: IDashboardRepo,
    generateSfDataEnv: GenerateSfDataEnv,
    generateDbtDataEnv: GenerateDbtDataEnv,
    updateSfDataEnv: UpdateSfDataEnv,
    buildDependencies: BuildDependencies,
  ) {
    this.#lineageRepo = lineageRepo;
    this.#logicRepo = logicRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
    this.#dashboardRepo = dashboardRepo;
    this.#generateSfDataEnv = generateSfDataEnv;
    this.#generateDbtDataEnv = generateDbtDataEnv;
    this.#updateSfDataEnv = updateSfDataEnv;
    this.#buildDependencies = buildDependencies;
  }

  #writeLineageToPersistence = async (lineage: Lineage): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#lineageRepo.insertOne(lineage, this.#auth, this.#connPool);
  };

  #writeWhResourcesToPersistence = async (dataEnv: DataEnv): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dataEnv.logicsToReplace && dataEnv.logicsToReplace.length)
      await this.#logicRepo.replaceMany(
        dataEnv.logicsToReplace,
        this.#auth,
        this.#connPool
      );
    if (dataEnv.matsToReplace && dataEnv.matsToReplace.length)
      await this.#materializationRepo.replaceMany(
        dataEnv.matsToReplace,
        this.#auth,
        this.#connPool
      );
    if (dataEnv.columnsToReplace && dataEnv.columnsToReplace.length)
      await this.#columnRepo.replaceMany(
        dataEnv.columnsToReplace,
        this.#auth,
        this.#connPool
      );

    if (dataEnv.logicsToCreate && dataEnv.logicsToCreate.length)
      await this.#logicRepo.insertMany(
        dataEnv.logicsToCreate,
        this.#auth,
        this.#connPool
      );

    if (dataEnv.matsToCreate && dataEnv.matsToCreate.length)
      await this.#materializationRepo.insertMany(
        dataEnv.matsToCreate,
        this.#auth,
        this.#connPool
      );

    if (dataEnv.columnsToCreate && dataEnv.columnsToCreate.length)
      await this.#columnRepo.insertMany(
        dataEnv.columnsToCreate,
        this.#auth,
        this.#connPool
      );

    if (dataEnv.logicToDeleteRefs && dataEnv.logicToDeleteRefs.length)
      await this.#logicRepo.deleteMany(
        dataEnv.logicToDeleteRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
      );

    if (dataEnv.matToDeleteRefs && dataEnv.matToDeleteRefs.length)
      await this.#materializationRepo.deleteMany(
        dataEnv.matToDeleteRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
      );

    if (dataEnv.columnToDeleteRefs && dataEnv.columnToDeleteRefs.length)
      await this.#columnRepo.deleteMany(
        dataEnv.columnToDeleteRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
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
        this.#connPool
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
        this.#connPool
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
      this.#connPool
    );
  };

  #genDbtDataEnv = async (): Promise<{dataEnv: DataEnv, catalog: ModelRepresentation[]}> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { dbtCatalog, dbtManifest } = this.#req;

    if (!dbtCatalog || !dbtManifest)
      throw new Error(
        'dbt resources missing. Cannot create dbt based data env'
      );

    const result = await this.#generateDbtDataEnv.execute(
      {
        dbtCatalog,
        dbtManifest,
        targetOrgId: this.#req.targetOrgId,
      },
      this.#auth,
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #genSfDataEnv = async (): Promise<{dataEnv: DataEnv, catalog: ModelRepresentation[]}> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Sf based lineage creation has to be invoked by user');

    const result = await this.#generateSfDataEnv.execute(
      null,
      { ...this.#auth, callerOrgId },
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #generateEnv = async (): Promise<{dataEnv: DataEnv, catalog: ModelRepresentation[]}> => {
    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...generating warehouse resources');

    const res =
      this.#req.dbtCatalog && this.#req.dbtManifest
        ? await this.#genDbtDataEnv()
        : await this.#genSfDataEnv();

    return res;
  };

  #refrSfDataEnv = async (lastLineageCompletedAt: string): Promise<{dataEnv: DataEnv, catalog: ModelRepresentation[]}> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const result = await this.#updateSfDataEnv.execute(
      { lastLineageCompletedAt },
      this.#auth,
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #refrDbtDataEnv = async (): Promise<{dataEnv: DataEnv, catalog: ModelRepresentation[]}> => {
    throw new Error('Not implemented');
  };

  #updateEnv = async (lastLineageCompletedAt: string): Promise<{dataEnv: DataEnv, catalog: ModelRepresentation[]}> => {
    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...updateing data env');

    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...generating warehouse resources');

    const result =
      this.#req.dbtCatalog && this.#req.dbtManifest
        ? await this.#refrDbtDataEnv()
        : await this.#refrSfDataEnv(lastLineageCompletedAt);

    return result;
  };

  #getNewDataEnv = async (): Promise<{
    dataEnv: DataEnv;
    operation: DataEnvOperation;
    catalog: ModelRepresentation[];
  }> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const latestLineage = await this.#lineageRepo.findLatest(
      { tolerateIncomplete: false },
      this.#auth,
      this.#connPool,
      this.#req.targetOrgId
    );

    let generateDataEnvResult: {dataEnv: DataEnv, catalog: ModelRepresentation[]};
    if (!latestLineage) generateDataEnvResult = await this.#generateEnv();
    else generateDataEnvResult = await this.#updateEnv(latestLineage.createdAt);

    return {
      ...generateDataEnvResult,
      operation: !latestLineage ? 'create' : 'update',
    };
  };

  #buildDeps = async (dataEnv: DataEnv, catalog: ModelRepresentation[]): Promise<BuildResult> => {
    if(!this.#auth  || !this.#connPool || !this.#req) throw new Error('Missing field values');

    console.log('...building dependencies');

    /* 
    todo-fix potential bug since resources that were deleted are not taken into account. 
    Also in case of data env update only a fraction of resources is available to build up dependencies 
    */
    const buildDependenciesResult = await this.#buildDependencies
      .execute(
        {
          logics: (dataEnv.logicsToCreate || []).concat((dataEnv.logicsToReplace ||  [])),
          mats: (dataEnv.matsToCreate || []).concat((dataEnv.matsToReplace || [])),
          columns: (dataEnv.columnsToCreate || []).concat((dataEnv.columnsToReplace || [])),
          catalog,
          targetOrgId: this.#req.targetOrgId,
          biToolType: this.#req.biTool,
        },
        this.#auth,
        this.#connPool
      );

    if(!buildDependenciesResult.success) throw new Error(buildDependenciesResult.error);
    if(!buildDependenciesResult.value) throw new Error('build dependencies usecase did not return val obj');

    return buildDependenciesResult.value;

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

      this.#connPool = connPool;
      this.#auth = auth;
      this.#req = req;

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      const lineage = Lineage.create({
        id: uuidv4(),
      });

      console.log('...writing lineage to persistence');
      await this.#writeLineageToPersistence(lineage);

      const { dataEnv, operation: dataEnvOperation, catalog } =
        await this.#getNewDataEnv();

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence(dataEnv);

      const { dashboards, dependencies } = await this.#buildDeps(dataEnv, catalog);

      console.log('...writing dashboards to persistence');
      await this.#writeDashboardsToPersistence(dashboards);

      console.log('...writing dependencies to persistence');
      await this.#writeDependenciesToPersistence(dependencies);

      console.log('...setting lineage complete state to true');
      await this.#updateLineage(lineage.id, { completed: true, diff: dataEnvOperation === 'update' ? JSON.stringify(dataEnv) : undefined });

      console.log('finished lineage creation.');

      return Result.ok(
        Lineage.build({
          id: lineage.id,
          createdAt: lineage.createdAt,
          completed: true,
          diff: dataEnvOperation === 'update' ? JSON.stringify(dataEnv) : undefined
        })
      );
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
