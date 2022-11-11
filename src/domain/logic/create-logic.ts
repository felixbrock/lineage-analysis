// todo clean architecture violation

import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ModelRepresentation, Logic, MaterializationDefinition } from '../entities/logic';
import { ILogicRepo } from './i-logic-repo';
import { ReadLogics } from './read-logics';
import {  } from '../services/i-db';
import { v4 as uuidv4 } from 'uuid';

interface DbtRequestProps {
  dbtDependentOn: MaterializationDefinition[];
}

interface GeneralRequestProps {
  relationName: string;
  sql: string;
  parsedLogic: string;
  lineageId: string;
  catalog: ModelRepresentation[];
  targetOrganizationId?: string;
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

export interface CreateLogicAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateLogicResponse = Result<Logic>;

export class CreateLogic
  implements
    IUseCase<
      CreateLogicRequestDto,
      CreateLogicResponse,
      CreateLogicAuthDto,
      
    >
{
  readonly #readLogics: ReadLogics;

  readonly #logicRepo: ILogicRepo;

  #: ;

  constructor(readLogics: ReadLogics, logicRepo: ILogicRepo) {
    this.#readLogics = readLogics;
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: CreateLogicRequestDto,
    auth: CreateLogicAuthDto,
    : 
  ): Promise<CreateLogicResponse> {
    const { dbtProps, generalProps: commonProps } = request.props;

    try {
      if (auth.isSystemInternal && !commonProps.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!commonProps.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (commonProps.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId: string;
      if (auth.isSystemInternal && commonProps.targetOrganizationId)
        organizationId = commonProps.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');

      this.# = ;

      const logic = Logic.create({
        generalProps: {
          id: uuidv4(),
          relationName: commonProps.relationName,

          sql: commonProps.sql,
          parsedLogic: commonProps.parsedLogic,
          lineageId: commonProps.lineageId,
          organizationId,
          catalog: commonProps.catalog,
        },
        dbtProps,
      });

      const readLogicsResult = await this.#readLogics.execute(
        {
          relationName: commonProps.relationName,
          lineageId: commonProps.lineageId,
          targetOrganizationId: commonProps.targetOrganizationId,
        },
        {
          isSystemInternal: auth.isSystemInternal,
          callerOrganizationId: auth.callerOrganizationId,
        },
        this.#
      );

      if (!readLogicsResult.success) throw new Error(readLogicsResult.error);
      if (!readLogicsResult.value) throw new Error('Reading logics failed');
      if (readLogicsResult.value.length)
        throw new ReferenceError('Logic to be created already exists');

      if (request.options.writeToPersistence)
        await this.#logicRepo.insertOne(logic, );

      return Result.ok(logic);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
