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
import { ILegacyLineageRepo, LineageUpdateDto } from '../i-lineage-repo';
import { ILegacyColumnRepo } from '../../column/i-column-repo';
import { ILegacyMaterializationRepo } from '../../materialization/i-materialization-repo';
import { IDependencyRepo } from '../../dependency/i-dependency-repo';
import { ILegacyLogicRepo } from '../../logic/i-logic-repo';
import { DbConnection } from '../../services/i-db';
import { QuerySfQueryHistory } from '../../integration-api/snowflake/query-snowflake-history';
import { Dashboard } from '../../entities/dashboard';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { IDashboardRepo } from '../../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import { buildLineage } from './build-lineage';
import { DbtDataEnvGenerator } from './dbt-data-env-generator';
import DependenciesBuilder from './dependencies-builder';
import { BiType } from '../../value-types/bilayer';
import DbtDataEnvMerger from './dbt/dbt-data-env-merger';
import { SfDataEnvGenerator } from './sf-data-env-generator';
import { QuerySnowflake } from '../../integration-api/snowflake/query-snowflake';
import SnowflakeDataEnvMerger from './snowflake/sf-data-env-merger';

export interface CreateLineageRequestDto {
  targetOrganizationId?: string;
  catalog?: string;
  manifest?: string;
  biType?: BiType;
}

interface DbtBasedBuildProps
  extends Omit<CreateLineageRequestDto, 'catalog' | 'manifest'> {
  catalog: string;
  manifest: string;
}

type SfBasedBuildProps = Omit<CreateLineageRequestDto, 'catalog' | 'manifest'>

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

  readonly #querySnowflake: QuerySnowflake;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  readonly #lineageRepo: ILegacyLineageRepo;

  readonly #logicRepo: ILegacyLogicRepo;

  readonly #materializationRepo: ILegacyMaterializationRepo;

  readonly #columnRepo: ILegacyColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  readonly #dashboardRepo: IDashboardRepo;

  readonly #readColumns: ReadColumns;

  #dbConnection: DbConnection;

  constructor(
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    createExternalDependency: CreateExternalDependency,
    parseSQL: ParseSQL,
    lineageRepo: ILegacyLineageRepo,
    logicRepo: ILegacyLogicRepo,
    materializationRepo: ILegacyMaterializationRepo,
    columnRepo: ILegacyColumnRepo,
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
  }): Promise<void> => {
    await this.#lineageRepo.insertOne(props.lineage, this.#dbConnection);

    if (props.matsToReplace.length)
      await this.#materializationRepo.replaceMany(
        props.matsToReplace,
        this.#dbConnection
      );
    if (props.columnsToReplace.length)
      await this.#columnRepo.replaceMany(
        props.columnsToReplace,
        this.#dbConnection
      );

    if (props.matsToCreate.length)
      await this.#materializationRepo.insertMany(
        props.matsToCreate,
        this.#dbConnection
      );

    if (props.columnsToCreate.length)
      await this.#columnRepo.insertMany(
        props.columnsToCreate,
        this.#dbConnection
      );
  };

  #legacyWriteWhResourcesToPersistence = async (props: {
    lineage: Lineage;
    matsToCreate: Materialization[];
    matsToReplace: Materialization[];
    columnsToCreate: Column[];
    columnsToReplace: Column[];
    logicsToCreate: Logic[];
    logicsToReplace: Logic[];
  }): Promise<void> => {
    await this.#lineageRepo.insertOne(props.lineage, this.#dbConnection);

    if (props.logicsToReplace.length)
      await this.#logicRepo.replaceMany(
        props.logicsToReplace,
        this.#dbConnection
      );
    if (props.matsToReplace.length)
      await this.#materializationRepo.replaceMany(
        props.matsToReplace,
        this.#dbConnection
      );
    if (props.columnsToReplace.length)
      await this.#columnRepo.replaceMany(
        props.columnsToReplace,
        this.#dbConnection
      );

    if (props.logicsToCreate.length)
      await this.#logicRepo.insertMany(
        props.logicsToCreate,
        this.#dbConnection
      );

    if (props.matsToCreate.length)
      await this.#materializationRepo.insertMany(
        props.matsToCreate,
        this.#dbConnection
      );

    if (props.columnsToCreate.length)
      await this.#columnRepo.insertMany(
        props.columnsToCreate,
        this.#dbConnection
      );
  };

  #writeDashboardsToPersistence = async (
    dashboards: Dashboard[]
  ): Promise<void> => {
    if (dashboards.length)
      await this.#dashboardRepo.insertMany(dashboards, this.#dbConnection);
  };

  #writeDependenciesToPersistence = async (
    dependencies: Dependency[]
  ): Promise<void> => {
    if (dependencies.length)
      await this.#dependencyRepo.insertMany(dependencies, this.#dbConnection);
  };

  #updateLineage = async (
    id: string,
    updateDto: LineageUpdateDto
  ): Promise<void> => {
    await this.#lineageRepo.updateOne(id, updateDto, this.#dbConnection);
  };

  #buildDbtBased = async (
    lineage: Lineage,
    organizationId: string,
    props: DbtBasedBuildProps,
    auth: CreateLineageAuthDto
  ): Promise<void> => {
    console.log('...generating warehouse resources');
    const { jwt, ...remainingAuth } = auth;
    const dataEnvGenerator = new DbtDataEnvGenerator(
      {
        dbtCatalog: props.catalog,
        dbtManifest: props.manifest,
        lineageId: lineage.id,
        targetOrganizationId: props.targetOrganizationId,
      },
      remainingAuth,
      this.#dbConnection,
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
    const dataEnvMerger = new DbtDataEnvMerger(
      { columns, materializations, logics, organizationId },
      this.#dbConnection,
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
        matDefinitions,
        organizationId,
        targetOrganizationId: props.targetOrganizationId,
      },
      auth,
      this.#dbConnection,
      {
        createDashboard: this.#createDashboard,
        createDependency: this.#createDependency,
        createExternalDependency: this.#createExternalDependency,
        readColumns: this.#readColumns,
        querySfQueryHistory: this.#querySfQueryHistory,
      }
    );
    const { dashboards, dependencies } = await dependenciesBuilder.build(
      props.biType
    );

    console.log('...writing dashboards to persistence');
    await this.#writeDashboardsToPersistence(dashboards);

    console.log('...writing dependencies to persistence');
    await this.#writeDependenciesToPersistence(dependencies);
  };

  #buildSfBased = async (
    lineage: Lineage,
    organizationId: string,
    props: SfBasedBuildProps,
    auth: CreateLineageAuthDto
  ): Promise<void> => {
    console.log('...generating warehouse resources');
    const dataEnvGenerator = new SfDataEnvGenerator(
      {
        lineageId: lineage.id,
        targetOrganizationId: props.targetOrganizationId,
      },
      auth,
      this.#dbConnection,
      {
        createColumn: this.#createColumn,
        createMaterialization: this.#createMaterialization,
        querySnowflake: this.#querySnowflake,
      }
    );
    const { materializations, columns } = await dataEnvGenerator.generate();

    console.log('...merging new lineage snapshot with last one');
    const dataEnvMerger = new SnowflakeDataEnvMerger(
      { columns, materializations, organizationId },
      {
        lineageRepo: this.#lineageRepo,
        columnRepo: this.#columnRepo,
        materializationRepo: this.#materializationRepo,
      }
    );

    const mergedDataEnv = await dataEnvMerger.merge();

    console.log('...writing dw resources to persistence');
    await this.#writeWhResourcesToPersistence({ lineage, ...mergedDataEnv });

    
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

      let organizationId: string;
      if (auth.callerOrganizationId) organizationId = auth.callerOrganizationId;
      else if (request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#dbConnection = dbConnection;

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      const lineage = buildLineage(organizationId);

      if (!!request.catalog !== !!request.manifest)
        throw new Error(
          'When creating lineage based on dbt both, the manifest and catalog file have to be provided'
        );

      const { catalog, manifest, ...remainingReq } = request;

      const dbtBased = catalog && manifest; 

      if(dbtBased)
          await this.#buildDbtBased(
            lineage,
            organizationId,
            { catalog, manifest, ...remainingReq },
            auth
          );
      else 
            await this.#buildSfBased(lineage, organizationId, request, auth);
      
      console.log('...setting lineage complete state to true');
      await this.#updateLineage(lineage.id, { completed: true });

      console.log('finished lineage creation.');

      return Result.ok(
        Lineage.build({
          id: lineage.id,
          organizationId: lineage.organizationId,
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
