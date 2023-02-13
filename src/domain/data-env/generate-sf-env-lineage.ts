// todo clean architecture violation
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { EnvLineage } from './env-lineage';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseGetSfEnvLineage from './base-get-sf-env-lineage';

export type GenerateSfEnvLineageRequestDto = undefined;

export interface GenerateSfEnvLineageAuthDto
  extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type GenerateSfEnvLineageResponse = Result<EnvLineage>;

export class GenerateSfEnvLineage
  extends BaseGetSfEnvLineage
  implements
    IUseCase<
      GenerateSfEnvLineageRequestDto,
      GenerateSfEnvLineageResponse,
      GenerateSfEnvLineageAuthDto,
      IConnectionPool
    >
{
  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateSfEnvLineageRequestDto,
    auth: GenerateSfEnvLineageAuthDto,
    connPool: IConnectionPool
  ): Promise<GenerateSfEnvLineageResponse> {
    try {
      this.connPool = connPool;
      this.auth = auth;

      const dependenciesToCreate = await this.generateDependencies();

      return Result.ok({
        dependenciesToCreate,
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
