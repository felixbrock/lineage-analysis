// todo clean architecture violation

import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  ModelRepresentation,
  Logic,
  MaterializationDefinition,
} from '../entities/logic';
import { ILogicRepo } from './i-logic-repo';
 
import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

interface DbtRequestProps {
  dbtDependentOn: MaterializationDefinition[];
}

interface GeneralRequestProps {
  relationName: string;
  sql: string;
  parsedLogic: string;
  lineageId: string;
  catalog: ModelRepresentation[];
  targetOrgId?: string;
}

export interface CreateLogicRequestDto {
  props: {
    generalProps: GeneralRequestProps;
    dbtProps?: DbtRequestProps;
  };
  options: {
    writeToPersistence: boolean;
  };
}

export type CreateLogicAuthDto = BaseAuth;

export type CreateLogicResponse = Result<Logic>;

export class CreateLogic
  implements
    IUseCase<CreateLogicRequestDto, CreateLogicResponse, CreateLogicAuthDto,IConnectionPool>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    req: CreateLogicRequestDto,
    auth: CreateLogicAuthDto,
    connPool: IConnectionPool
  ): Promise<CreateLogicResponse> {
    const { dbtProps, generalProps: commonProps } = req.props;

    try {
      if (auth.isSystemInternal && !commonProps.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!commonProps.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (commonProps.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const logic = Logic.create({
        generalProps: {
          id: uuidv4(),
          relationName: commonProps.relationName,

          sql: commonProps.sql,
          parsedLogic: commonProps.parsedLogic,
          lineageId: commonProps.lineageId,
          catalog: commonProps.catalog,
        },
        dbtProps,
      });

      if (req.options.writeToPersistence)
        await this.#logicRepo.insertOne(
          logic,
          auth,
          connPool,
        );

      return Result.ok(logic);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.error(error.stack);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
