// todo clean architecture violation
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { DataEnv } from './data-env';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseGetSfDataEnv, { ColumnRepresentation } from './base-get-sf-data-env';

export interface GenerateSfDataEnvRequestDto {
  lineageId: string;
}

export interface GenerateSfDataEnvAuthDto
  extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type GenerateSfDataEnvResponse = Result<DataEnv>;

export class GenerateSfDataEnv
  extends BaseGetSfDataEnv
  implements
    IUseCase<
      GenerateSfDataEnvRequestDto,
      GenerateSfDataEnvResponse,
      GenerateSfDataEnvAuthDto,
      IConnectionPool
    >
{
  static #groupByRelationName = <T extends { relationName: string }>(
    accumulation: { [key: string]: T[] },
    element: T
  ): { [key: string]: T[] } => {
    const localAcc = accumulation;

    const key = element.relationName;
    if (!(key in accumulation)) {
      localAcc[key] = [];
    }
    localAcc[key].push(element);
    return localAcc;
  };

  generateDbResources = async (base: string): Promise<void> => {
    const matRepresentations = await this.getMatRepresentations(base);
    const columnRepresentations = await this.getColumnRepresentations(base);

    const colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    } = columnRepresentations.reduce(
      GenerateSfDataEnv.#groupByRelationName,
      {}
    );

    this.generateCatalog(matRepresentations, colRepresentationsByRelationName);

    await Promise.all(
      matRepresentations.map(async (el) => {
        const options = {
          writeToPersistence: false,
        };

        const logicRepresentation = await this.getLogicRepresentation(
          el.type === 'view' ? 'view' : 'table',
          el.name,
          el.schemaName,
          el.databaseName
        );

        await this.generateDWResource(
          {
            matRepresentation: el,
            logicRepresentation,
            columnRepresentations:
              colRepresentationsByRelationName[el.relationName],
            relationName: el.relationName,
          },
          options
        );
      })
    );
  };

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateSfDataEnvRequestDto,
    auth: GenerateSfDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<GenerateSfDataEnvResponse> {
    try {
      this.connPool = connPool;
      this.auth = auth;
      this.lineageId = req.lineageId;

      const dbRepresentations = await this.getDbRepresentations(connPool, auth);

      await Promise.all(
        dbRepresentations.map(async (el) => {
          await this.generateDbResources(el.name);
        })
      );

      return Result.ok({
        matsToCreate: this.materializations,
        columnsToCreate: this.columns,
        logicsToCreate: this.logics,
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
