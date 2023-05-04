// todo clean architecture violation
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { ExternalDataEnvProps } from './external-data-env';
import BaseGetSfExternalDataEnv from './base-get-sf-external-data-env';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { BiToolType } from '../value-types/bi-tool';
import { CreateDashboards } from '../dashboard/create-dashboards';
import { CreateDependencies } from '../dependency/create-dependencies';
import { QuerySfQueryHistory } from '../snowflake-api/query-snowflake-history';
import { IDb } from '../services/i-db';
import GetSfExternalDataEnvRepo from '../../infrastructure/persistence/get-sf-external-data-env-repo';

export type GenerateSfExternalDataEnvRequestDto = {
  targetOrgId?: string;
  biToolType?: BiToolType;
};

export type GenerateSfExternalDataEnvAuthDto = BaseAuth;

export type GenerateSfExternalDataEnvResponse = Result<ExternalDataEnvProps>;

export class GenerateSfExternalDataEnv
  extends BaseGetSfExternalDataEnv
  implements
    IUseCase<
      GenerateSfExternalDataEnvRequestDto,
      GenerateSfExternalDataEnvResponse,
      GenerateSfExternalDataEnvAuthDto,
      IDb
    >
{
  constructor(
    createDashboards: CreateDashboards,
    createDependencies: CreateDependencies,
    querySfQueryHistory: QuerySfQueryHistory,
    querySnowflake: QuerySnowflake,
    getSfExternalDataEnvRepo: GetSfExternalDataEnvRepo
  ) {
    super(
      querySnowflake,
      querySfQueryHistory,
      createDashboards,
      createDependencies,
      getSfExternalDataEnvRepo
    );
  }

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateSfExternalDataEnvRequestDto,
    auth: GenerateSfExternalDataEnvAuthDto,
    db: IDb
  ): Promise<GenerateSfExternalDataEnvResponse> {
    try {
      this.connPool = db.sfConnPool;
      this.dbConnection = db.mongoConn;
      this.auth = auth;
      this.targetOrgId = req.targetOrgId;

      const result = await this.buildBiResources(req.biToolType);

      return Result.ok({
        dataEnv: {
          dashboardsToCreate: result.dashboards,
          dashboardsToReplace: [],
          dashboardToDeleteRefs: [],
          dependenciesToCreate: result.dependencies,
          dependencyToDeleteRefs: [],
          deleteAllOldDependencies: false,
        },
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
