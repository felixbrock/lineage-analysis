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
import {
  ColToDeleteRef,
  DataEnv,
  DataEnvDto,
  DataEnvProps,
  LogicToDeleteRef,
  MatToDeleteRef,
} from '../data-env/data-env';
import { GenerateDbtDataEnv } from '../data-env/generate-dbt-data-env';
import { GenerateSfDataEnv } from '../data-env/generate-sf-data-env';
import { UpdateSfDataEnv } from '../data-env/update-sf-data-env';
import { Logic } from '../entities/logic';
import { IObservabilityApiRepo } from '../observability-api/i-observability-api-repo';
import {
  DashboardToDeleteRef,
  ExternalDataEnv,
  ExternalDataEnvProps,
} from '../external-data-env/external-data-env';
import { UpdateSfExternalDataEnv } from '../external-data-env/update-sf-external-data-env';
import { GenerateSfExternalDataEnv } from '../external-data-env/generate-sf-external-data-env';
import { Materialization } from '../entities/materialization';
import { Column } from '../entities/column';

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

  readonly #observabilityApiRepo: IObservabilityApiRepo;

  readonly #generateSfDataEnv: GenerateSfDataEnv;

  readonly #generateSfExternalDataEnv: GenerateSfExternalDataEnv;

  readonly #generateDbtDataEnv: GenerateDbtDataEnv;

  readonly #updateSfDataEnv: UpdateSfDataEnv;

  readonly #updateSfExternalDataEnv: UpdateSfExternalDataEnv;

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
    observabilityApiRepo: IObservabilityApiRepo,
    generateSfDataEnv: GenerateSfDataEnv,
    generateSfExternalDataEnv: GenerateSfExternalDataEnv,
    generateDbtDataEnv: GenerateDbtDataEnv,
    updateSfDataEnv: UpdateSfDataEnv,
    updateSfExternalDataEnv: UpdateSfExternalDataEnv
  ) {
    this.#lineageRepo = lineageRepo;
    this.#logicRepo = logicRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
    this.#dashboardRepo = dashboardRepo;
    this.#observabilityApiRepo = observabilityApiRepo;
    this.#generateSfDataEnv = generateSfDataEnv;
    this.#generateSfExternalDataEnv = generateSfExternalDataEnv;
    this.#generateDbtDataEnv = generateDbtDataEnv;
    this.#updateSfDataEnv = updateSfDataEnv;
    this.#updateSfExternalDataEnv = updateSfExternalDataEnv;
  }

  #writeLineageToPersistence = async (lineage: Lineage): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#lineageRepo.insertOne(lineage, this.#auth, this.#connPool);
  };

  #createWhResourcesInPersistence = async (
    logics: Logic[],
    mats: Materialization[],
    cols: Column[],
    dependencies: Dependency[]
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (logics.length)
      await this.#logicRepo.insertMany(logics, this.#auth, this.#connPool);

    if (mats.length)
      await this.#materializationRepo.insertMany(
        mats,
        this.#auth,
        this.#connPool
      );

    if (cols.length)
      await this.#columnRepo.insertMany(cols, this.#auth, this.#connPool);

    if (dependencies.length)
      await this.#dependencyRepo.insertMany(
        dependencies,
        this.#auth,
        this.#connPool
      );
  };

  #replaceWhResourcesInPersistence = async (
    logics: Logic[],
    mats: Materialization[],
    cols: Column[]
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (logics.length)
      await this.#logicRepo.replaceMany(logics, this.#auth, this.#connPool);
    if (mats.length)
      await this.#materializationRepo.replaceMany(
        mats,
        this.#auth,
        this.#connPool
      );
    if (cols.length)
      await this.#columnRepo.replaceMany(cols, this.#auth, this.#connPool);
  };

  #deleteTestSuites = async (
    targetResourceIds: string[],
    jwt: string
  ): Promise<void> => {
    await this.#observabilityApiRepo.deleteQuantTestSuites(
      jwt,
      targetResourceIds,
      'soft'
    );
    await this.#observabilityApiRepo.deleteQualTestSuites(
      jwt,
      targetResourceIds,
      'soft'
    );
  };

  #deleteWhResourcesFromPersistence = async (
    logicRefs: LogicToDeleteRef[],
    matRefs: MatToDeleteRef[],
    colRefs: ColToDeleteRef[],
    deleteAllDependencies: boolean
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (deleteAllDependencies) {
      await this.#dependencyRepo.deleteAll(this.#auth, this.#connPool);
    }

    if (logicRefs.length)
      await this.#logicRepo.deleteMany(
        logicRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
      );

    const targetResourceIds: string[] = [];
    if (matRefs.length) {
      await this.#materializationRepo.deleteMany(
        matRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
      );

      targetResourceIds.push(...matRefs.map((el) => el.id));
    }

    if (colRefs.length) {
      await this.#columnRepo.deleteMany(
        colRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
      );

      targetResourceIds.push(...colRefs.map((el) => el.id));
    }

    if (targetResourceIds.length)
      await this.#deleteTestSuites(targetResourceIds, this.#auth.jwt);
  };

  #writeWhResourcesToPersistence = async (dataEnv: DataEnv): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#deleteWhResourcesFromPersistence(
      dataEnv.logicToDeleteRefs,
      dataEnv.matToDeleteRefs,
      dataEnv.columnToDeleteRefs,
      dataEnv.deleteAllOldDependencies
    );

    await this.#replaceWhResourcesInPersistence(
      dataEnv.logicsToReplace,
      dataEnv.matsToReplace,
      dataEnv.columnsToReplace
    );

    await this.#createWhResourcesInPersistence(
      dataEnv.logicsToCreate,
      dataEnv.matsToCreate,
      dataEnv.columnsToCreate,
      dataEnv.dependenciesToCreate
    );
  };

  #createExternalResourcesInPersistence = async (
    dashboards: Dashboard[],
    dependencies: Dependency[]
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dashboards.length)
      await this.#dashboardRepo.insertMany(
        dashboards,
        this.#auth,
        this.#connPool
      );

    if (dependencies.length)
      await this.#dependencyRepo.insertMany(
        dependencies,
        this.#auth,
        this.#connPool
      );
  };

  #replaceExternalResourcesInPersistence = async (
    dashboards: Dashboard[]
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dashboards.length)
      await this.#dashboardRepo.replaceMany(
        dashboards,
        this.#auth,
        this.#connPool
      );
  };

  #deleteExternalResourcesFromPersistence = async (
    dashboardToDeleteRefs: DashboardToDeleteRef[],
    deleteAllDependencies: boolean
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (deleteAllDependencies) {
      await this.#dependencyRepo.deleteAll(this.#auth, this.#connPool);
    }

    if (dashboardToDeleteRefs.length)
      await this.#dashboardRepo.deleteMany(
        dashboardToDeleteRefs.map((el) => el.id),
        this.#auth,
        this.#connPool
      );
  };

  #writeExternalResourcesToPersistence = async (
    dataEnv: ExternalDataEnv
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#deleteExternalResourcesFromPersistence(
      dataEnv.dashboardToDeleteRefs,
      dataEnv.deleteAllOldDependencies
    );

    await this.#replaceExternalResourcesInPersistence(
      dataEnv.dashboardsToReplace
    );

    await this.#createExternalResourcesInPersistence(
      dataEnv.dashboardsToCreate,
      dataEnv.dependenciesToCreate
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

  #genDbtExternalDataEnv = async (): Promise<ExternalDataEnvProps> => {
    throw new Error('Not implemented');
  };

  #genDbtDataEnv = async (): Promise<DataEnvProps> => {
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

  #genSfDataEnv = async (): Promise<DataEnvProps> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Sf based lineage creation has to be invoked by user');

    const result = await this.#generateSfDataEnv.execute(
      undefined,
      { ...this.#auth, callerOrgId },
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #genSfExternalDataEnv = async (
    biToolType: BiToolType
  ): Promise<ExternalDataEnvProps> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Sf based lineage creation has to be invoked by user');

    const result = await this.#generateSfExternalDataEnv.execute(
      { biToolType },
      { ...this.#auth, callerOrgId },
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #generateExternalEnv = async (
    biToolType: BiToolType
  ): Promise<ExternalDataEnvProps> => {
    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...generating external data environment');

    const res =
      this.#req.dbtCatalog && this.#req.dbtManifest
        ? await this.#genDbtExternalDataEnv()
        : await this.#genSfExternalDataEnv(biToolType);

    return res;
  };

  #generateEnv = async (): Promise<DataEnvProps> => {
    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...generating warehouse resources');

    const res =
      this.#req.dbtCatalog && this.#req.dbtManifest
        ? await this.#genDbtDataEnv()
        : await this.#genSfDataEnv();

    return res;
  };

  #updSfExternalDataEnv = async (
    latestLineageCompletedAt: string,
    biToolType: BiToolType
  ): Promise<ExternalDataEnvProps> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Caller Org Id required to update Sf data env');

    const result = await this.#updateSfExternalDataEnv.execute(
      {
        latestLineage: {
          completedAt: latestLineageCompletedAt,
        },
        biToolType,
      },
      { ...this.#auth, callerOrgId },
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #updSfDataEnv = async (
    latestLineageCompletedAt: string,
    latestLineagedbCoveredNames: string[]
  ): Promise<DataEnvProps> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Caller Org Id required to update Sf data env');

    const result = await this.#updateSfDataEnv.execute(
      {
        latestLineage: {
          completedAt: latestLineageCompletedAt,
          dbCoveredNames: latestLineagedbCoveredNames,
        },
      },
      { ...this.#auth, callerOrgId },
      this.#connPool
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #updDbtDataEnv = async (): Promise<DataEnvProps> => {
    throw new Error('Not implemented');
  };

  #updDbtExternalDataEnv = async (): Promise<ExternalDataEnvProps> => {
    throw new Error('Not implemented');
  };

  #updateExternalEnv = async (
    latestLineageCompletedAt: string,
    biToolType: BiToolType
  ): Promise<ExternalDataEnvProps> => {
    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...updating external data env');

    const result =
      this.#req.dbtCatalog && this.#req.dbtManifest
        ? await this.#updDbtExternalDataEnv()
        : await this.#updSfExternalDataEnv(
            latestLineageCompletedAt,
            biToolType
          );

    return result;
  };

  #updateEnv = async (
    latestLineageCompletedAt: string,
    latestLineageDbCoveredNames: string[]
  ): Promise<DataEnvProps> => {
    if (!this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...updating data env');

    const result =
      this.#req.dbtCatalog && this.#req.dbtManifest
        ? await this.#updDbtDataEnv()
        : await this.#updSfDataEnv(
            latestLineageCompletedAt,
            latestLineageDbCoveredNames
          );

    return result;
  };

  #getNewDataEnv = async (
    latestLineage?: Lineage
  ): Promise<DataEnvProps & { operation: DataEnvOperation }> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...building new data env');

    let generateDataEnvResult: DataEnvProps;
    if (!latestLineage) generateDataEnvResult = await this.#generateEnv();
    else
      generateDataEnvResult = await this.#updateEnv(
        latestLineage.createdAt,
        latestLineage.dbCoveredNames
      );

    return {
      ...generateDataEnvResult,
      operation: !latestLineage ? 'create' : 'update',
    };
  };

  #getNewExternalDataEnv = async (
    biToolType: BiToolType,
    latestLineage?: Lineage
  ): Promise<ExternalDataEnvProps & { operation: DataEnvOperation }> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing field values');

    console.log('...building new external data env');

    let getExternalDataEnv: ExternalDataEnvProps;
    if (!latestLineage)
      getExternalDataEnv = await this.#generateExternalEnv(biToolType);
    else
      getExternalDataEnv = await this.#updateExternalEnv(
        latestLineage.createdAt,
        biToolType
      );

    return {
      ...getExternalDataEnv,
      operation: !latestLineage ? 'create' : 'update',
    };
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

      const latestLineage = await this.#lineageRepo.findLatest(
        { tolerateIncomplete: false },
        this.#auth,
        this.#connPool,
        this.#req.targetOrgId
      );

      const {
        dataEnv,
        operation: dataEnvOperation,
        dbCoveredNames,
      } = await this.#getNewDataEnv(latestLineage);

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence(dataEnv);

      if (req.biTool) {
        const { dataEnv: externalDataEnv } = await this.#getNewExternalDataEnv(
          req.biTool,
          latestLineage
        );

        console.log('...writing external resources to persistence');
        await this.#writeExternalResourcesToPersistence(externalDataEnv);
      }

      let dataEnvDto: DataEnvDto | undefined;
      if (dataEnvOperation === 'update')
        dataEnvDto = {
          matToCreateRelationNames: dataEnv.matsToCreate.map(
            (el) => `${el.databaseName}.${el.schemaName}.${el.name}`
          ),
          matToDeleteRelationNames: dataEnv.matToDeleteRefs.map(
            (el) => `${el.dbName}.${el.schemaName}.${el.name}`
          ),
          matToReplaceRelationNames: dataEnv.matsToReplace.map(
            (el) => `${el.databaseName}.${el.schemaName}.${el.name}`
          ),
        };

      console.log('...setting lineage complete state to true');
      await this.#updateLineage(lineage.id, {
        completed: true,
        dbCoveredNames,
        diff: dataEnvDto ? JSON.stringify(dataEnvDto) : undefined,
      });

      console.log('finished lineage creation.');

      return Result.ok(
        Lineage.build({
          id: lineage.id,
          createdAt: lineage.createdAt,
          completed: true,
          dbCoveredNames,
          diff: dataEnvDto ? JSON.stringify(dataEnvDto) : undefined,
        })
      );
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
