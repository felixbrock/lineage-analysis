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
import { GenerateSfEnvLineage } from '../data-env/generate-sf-env-lineage';
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
import { EnvLineage } from '../data-env/env-lineage';
import { IDb, IDbConnection } from '../services/i-db';

export interface CreateLineageRequestDto {
  targetOrgId?: string;
  dbtCatalog?: string;
  dbtManifest?: string;
  biTool?: BiToolType;
}

export interface CreateLineageAuthDto extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}
export type CreateLineageResponseDto = Result<string>;

type DataEnvOperation = 'create' | 'update';

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto,
      IDb
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

  readonly #generateSfEnvLineage: GenerateSfEnvLineage;

  #auth?: CreateLineageAuthDto;

  #connPool?: IConnectionPool;

  #dbConnection?: IDbConnection;

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
    generateSfEnvLineage: GenerateSfEnvLineage,
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
    this.#generateSfEnvLineage = generateSfEnvLineage;
    this.#updateSfDataEnv = updateSfDataEnv;
    this.#updateSfExternalDataEnv = updateSfExternalDataEnv;
  }

  #writeLineageToPersistence = async (lineage: Lineage): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#lineageRepo.insertOne(lineage, this.#auth, this.#dbConnection);
  };

  #createWhResourcesInPersistence = async (
    logics: Logic[],
    mats: Materialization[],
    cols: Column[]
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (logics.length)
      await this.#logicRepo.insertMany(logics, this.#auth, this.#dbConnection);

    if (mats.length)
      await this.#materializationRepo.insertMany(
        mats,
        this.#auth,
        this.#dbConnection
      );

    if (cols.length)
      await this.#columnRepo.insertMany(cols, this.#auth, this.#dbConnection);
  };

  #replaceWhResourcesInPersistence = async (
    logics: Logic[],
    mats: Materialization[],
    cols: Column[]
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (logics.length)
      await this.#logicRepo.replaceMany(logics, this.#auth, this.#dbConnection);
    if (mats.length)
      await this.#materializationRepo.replaceMany(
        mats,
        this.#auth,
        this.#dbConnection
      );
    if (cols.length)
      await this.#columnRepo.replaceMany(cols, this.#auth, this.#dbConnection);
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
    colRefs: ColToDeleteRef[]
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (logicRefs.length)
      await this.#logicRepo.deleteMany(
        logicRefs.map((el) => el.id),
        this.#auth,
        this.#dbConnection
      );

    const targetResourceIds: string[] = [];
    if (matRefs.length) {
      await this.#materializationRepo.deleteMany(
        matRefs.map((el) => el.id),
        this.#auth,
        this.#dbConnection
      );

      targetResourceIds.push(...matRefs.map((el) => el.id));
    }

    if (colRefs.length) {
      await this.#columnRepo.deleteMany(
        colRefs.map((el) => el.id),
        this.#auth,
        this.#dbConnection
      );

      targetResourceIds.push(...colRefs.map((el) => el.id));
    }

    if (targetResourceIds.length)
      await this.#deleteTestSuites(targetResourceIds, this.#auth.jwt);
  };

  #writeWhResourcesToPersistence = async (dataEnv: DataEnv): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#deleteWhResourcesFromPersistence(
      dataEnv.logicToDeleteRefs,
      dataEnv.matToDeleteRefs,
      dataEnv.columnToDeleteRefs
    );

    await this.#replaceWhResourcesInPersistence(
      dataEnv.logicsToReplace,
      dataEnv.matsToReplace,
      dataEnv.columnsToReplace
    );

    await this.#createWhResourcesInPersistence(
      dataEnv.logicsToCreate,
      dataEnv.matsToCreate,
      dataEnv.columnsToCreate
    );
  };

  #createExternalResourcesInPersistence = async (
    dashboards: Dashboard[],
    dependencies: Dependency[]
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dashboards.length)
      await this.#dashboardRepo.insertMany(
        dashboards,
        this.#auth,
        this.#dbConnection
      );

    if (dependencies.length)
      await this.#dependencyRepo.insertMany(
        dependencies,
        this.#auth,
        this.#dbConnection
      );
  };

  #replaceExternalResourcesInPersistence = async (
    dashboards: Dashboard[]
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (dashboards.length)
      await this.#dashboardRepo.replaceMany(
        dashboards,
        this.#auth,
        this.#dbConnection
      );
  };

  #deleteExternalResourcesFromPersistence = async (
    dashboardToDeleteRefs: DashboardToDeleteRef[],
    deleteAllDependencies: boolean
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    if (deleteAllDependencies) {
      await this.#dependencyRepo.deleteAll(this.#auth, this.#dbConnection);
    }

    if (dashboardToDeleteRefs.length)
      await this.#dashboardRepo.deleteMany(
        dashboardToDeleteRefs.map((el) => el.id),
        this.#auth,
        this.#dbConnection
      );
  };

  #writeExternalResourcesToPersistence = async (
    dataEnv: ExternalDataEnv
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection || !this.#req)
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
    if (!this.#auth || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    await this.#lineageRepo.updateOne(
      id,
      updateDto,
      this.#auth,
      this.#dbConnection
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
    if (!this.#auth || !this.#connPool || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Sf based lineage creation has to be invoked by user');

    const result = await this.#generateSfDataEnv.execute(
      undefined,
      { ...this.#auth, callerOrgId },
      { mongoConn: this.#dbConnection, sfConnPool: this.#connPool }
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #genSfExternalDataEnv = async (
    biToolType?: BiToolType
  ): Promise<ExternalDataEnvProps> => {
    if (!this.#auth || !this.#connPool || !this.#dbConnection || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Sf based lineage creation has to be invoked by user');

    const result = await this.#generateSfExternalDataEnv.execute(
      { biToolType },
      { ...this.#auth, callerOrgId },
      { mongoConn: this.#dbConnection, sfConnPool: this.#connPool }
    );

    if (!result.success) throw new Error(result.error);
    if (!result.value)
      throw new Error('Missing value obj after generating dbt data env');

    return result.value;
  };

  #generateExternalEnv = async (
    biToolType?: BiToolType
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

  #writeSfEnvLineageToPersistence = async (
    dependencies: Dependency[]
  ): Promise<void> => {
    if (!this.#auth || !this.#dbConnection)
      throw new Error('Missing properties detected when creating lineage');

    await this.#dependencyRepo.deleteAll(this.#auth, this.#dbConnection);

    await this.#dependencyRepo.insertMany(
      dependencies,
      this.#auth,
      this.#dbConnection
    );
  };

  #genSfEnvLineage = async (): Promise<EnvLineage> => {
    if (!this.#auth || !this.#connPool || !this.#dbConnection)
      throw new Error('Missing properties detected when creating lineage');

    const generateSfEnvLineageResult = await this.#generateSfEnvLineage.execute(
      undefined,
      this.#auth,
      { sfConnPool: this.#connPool, mongoConn: this.#dbConnection }
    );

    if (!generateSfEnvLineageResult.success)
      throw new Error(generateSfEnvLineageResult.error);
    if (!generateSfEnvLineageResult.value)
      throw new Error('Missing value obj after generating sf env lineage');

    const sfEnvLineage = generateSfEnvLineageResult.value;

    return sfEnvLineage;
  };

  #updSfExternalDataEnv = async (
    latestLineageCompletedAt: string,
    biToolType?: BiToolType
  ): Promise<ExternalDataEnvProps> => {
    if (!this.#auth || !this.#dbConnection || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Caller Org Id required to update Sf data env');

    const result = await this.#updateSfExternalDataEnv.execute(
      {
        latestCompletedLineage: {
          completedAt: latestLineageCompletedAt,
        },
        biToolType,
      },
      { ...this.#auth, callerOrgId },
      { mongoConn: this.#dbConnection, sfConnPool: this.#connPool }
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
    if (!this.#auth || !this.#connPool || !this.#req || !this.#dbConnection)
      throw new Error('Missing properties detected when creating lineage');

    const { callerOrgId } = this.#auth;
    if (!callerOrgId)
      throw new Error('Caller Org Id required to update Sf data env');

    const result = await this.#updateSfDataEnv.execute(
      {
        latestCompletedLineage: {
          completedAt: latestLineageCompletedAt,
          dbCoveredNames: latestLineagedbCoveredNames,
        },
      },
      { ...this.#auth, callerOrgId },
      { sfConnPool: this.#connPool, mongoConn: this.#dbConnection}
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
    biToolType?: BiToolType
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
    latestCompletedLineage?: Lineage
  ): Promise<DataEnvProps & { operation: DataEnvOperation }> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing properties detected when creating lineage');

    console.log('...building new data env');

    let generateDataEnvResult: DataEnvProps;
    if (!latestCompletedLineage)
      generateDataEnvResult = await this.#generateEnv();
    else
      generateDataEnvResult = await this.#updateEnv(
        latestCompletedLineage.createdAt,
        latestCompletedLineage.dbCoveredNames
      );

    return {
      ...generateDataEnvResult,
      operation: !latestCompletedLineage ? 'create' : 'update',
    };
  };

  #getNewExternalDataEnv = async (
    biToolType?: BiToolType,
    latestCompletedLineage?: Lineage
  ): Promise<ExternalDataEnvProps & { operation: DataEnvOperation }> => {
    if (!this.#auth || !this.#connPool || !this.#req)
      throw new Error('Missing field values');

    console.log('...building new external data env');

    let getExternalDataEnv: ExternalDataEnvProps;
    if (!latestCompletedLineage)
      getExternalDataEnv = await this.#generateExternalEnv(biToolType);
    else
      getExternalDataEnv = await this.#updateExternalEnv(
        latestCompletedLineage.createdAt,
        biToolType
      );

    return {
      ...getExternalDataEnv,
      operation: !latestCompletedLineage ? 'create' : 'update',
    };
  };

  async execute(
    req: CreateLineageRequestDto,
    auth: CreateLineageAuthDto,
    db: IDb
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

      this.#connPool = db.sfConnPool;
      this.#dbConnection = db.mongoConn;
      this.#auth = auth;
      this.#req = req;

      const latestLineage = await this.#lineageRepo.findLatest(
        { tolerateIncomplete: true },
        this.#auth,
        this.#dbConnection,
        this.#req.targetOrgId
      );

      const latestCompletedLineage =
        latestLineage && latestLineage.creationState === 'completed'
          ? latestLineage
          : await this.#lineageRepo.findLatest(
              { tolerateIncomplete: false },
              this.#auth,
              this.#dbConnection,
              this.#req.targetOrgId
            );

      let lineage: Lineage;
      if (!latestLineage || latestLineage.creationState === 'completed') {
        console.log('starting lineage creation...');

        console.log('...building lineage object');
        lineage = Lineage.create({
          id: uuidv4(),
        });

        console.log('...writing lineage to persistence');
        await this.#writeLineageToPersistence(lineage);
      } else lineage = latestLineage;

      if (
        lineage.creationState !== 'wh-resources-done' &&
        lineage.creationState !== 'internal-lineage-done'
      ) {
        const {
          dataEnv,
          operation: dataEnvOperation,
          dbCoveredNames,
        } = await this.#getNewDataEnv(latestCompletedLineage);

        console.log('...writing dw resources to persistence');
        await this.#writeWhResourcesToPersistence(dataEnv);

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

        console.log('...updating lineage creation state');
        await this.#updateLineage(lineage.id, {
          creationState: 'wh-resources-done',
          dbCoveredNames,
          diff: dataEnvDto ? JSON.stringify(dataEnvDto) : undefined,
        });
      }

      if (lineage.creationState !== 'internal-lineage-done') {
        console.log('...generating new sf lineage');
        const envLineage = await this.#genSfEnvLineage();

        console.log('...writing new sf lineage to persistence');
        await this.#writeSfEnvLineageToPersistence(
          envLineage.dependenciesToCreate
        );

        console.log('...updating lineage creation state');
        await this.#updateLineage(lineage.id, {
          creationState: 'internal-lineage-done',
        });
      }

      const { dataEnv: externalDataEnv } = await this.#getNewExternalDataEnv(
        req.biTool,
        latestCompletedLineage
      );

      console.log('...writing external resources to persistence');
      await this.#writeExternalResourcesToPersistence(externalDataEnv);

      console.log('...setting lineage complete state to true');
      await this.#updateLineage(lineage.id, {
        creationState: 'completed',
      });

      console.log('finished lineage creation.');

      return Result.ok(lineage.id);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
