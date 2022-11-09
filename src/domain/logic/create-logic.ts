// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ModelRepresentation, Logic, MaterializationDefinition } from '../entities/logic';
import { ILegacyLogicRepo } from './i-logic-repo';
import { ReadLogics } from './read-logics';
import { DbConnection } from '../services/i-db';

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
      DbConnection
    >
{
  readonly #readLogics: ReadLogics;

  readonly #logicRepo: ILegacyLogicRepo;

  #dbConnection: DbConnection;

  constructor(readLogics: ReadLogics, logicRepo: ILegacyLogicRepo) {
    this.#readLogics = readLogics;
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: CreateLogicRequestDto,
    auth: CreateLogicAuthDto,
    dbConnection: DbConnection
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

      this.#dbConnection = dbConnection;

      const logic = Logic.create({
        generalProps: {
          id: new ObjectId().toHexString(),
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
        this.#dbConnection
      );

      if (!readLogicsResult.success) throw new Error(readLogicsResult.error);
      if (!readLogicsResult.value) throw new Error('Reading logics failed');
      if (readLogicsResult.value.length)
        throw new ReferenceError('Logic to be created already exists');

      if (request.options.writeToPersistence)
        await this.#logicRepo.insertOne(logic, dbConnection);

      return Result.ok(logic);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
