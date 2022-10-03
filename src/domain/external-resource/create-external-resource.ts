import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  ExternalResource,
  ExternalResourceType,
} from '../entities/external-resource';
import { ReadExternalResources } from './read-external-resources';
import { IExternalResourceRepo } from './i-external-resource-repo';
import { DbConnection } from '../services/i-db';

export interface ExternalResourceRequestObj {
  name: string;
  type: ExternalResourceType;
  lineageId: string;
  targetOrganizationId?: string;
}

export interface CreateExternalResourceRequestDto {
  requestObj: ExternalResourceRequestObj;
  writeToPersistence: boolean;
}

export interface CreateExternalResourceAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateExternalResourceResponseDto = Result<ExternalResource>;

export class CreateExternalResource
  implements
    IUseCase<
      CreateExternalResourceRequestDto,
      CreateExternalResourceResponseDto,
      CreateExternalResourceAuthDto,
      DbConnection
    >
{
  readonly #externalresourceRepo: IExternalResourceRepo;

  readonly #readExternalResources: ReadExternalResources;

  #dbConnection: DbConnection;

  constructor(
    readExternalResources: ReadExternalResources,
    externalresourceRepo: IExternalResourceRepo
  ) {
    this.#readExternalResources = readExternalResources;
    this.#externalresourceRepo = externalresourceRepo;
  }

  async execute(
    request: CreateExternalResourceRequestDto,
    auth: CreateExternalResourceAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateExternalResourceResponseDto> {
    try {
      if (auth.isSystemInternal && !request.requestObj.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (
        !request.requestObj.targetOrganizationId &&
        !auth.callerOrganizationId
      )
        throw new Error('No organization Id instance provided');
      if (request.requestObj.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId: string;
      if (auth.isSystemInternal && request.requestObj.targetOrganizationId)
        organizationId = request.requestObj.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');

      this.#dbConnection = dbConnection;

      const externalresource = ExternalResource.create({
        id: new ObjectId().toHexString(),
        name: request.requestObj.name,
        type: request.requestObj.type,
        lineageId: request.requestObj.lineageId,
        organizationId,
      });

      const readExternalResourcesResult =
        await this.#readExternalResources.execute(
          {
            name: request.requestObj.name,
            type: request.requestObj.type,
            lineageId: request.requestObj.lineageId,
            targetOrganizationId: request.requestObj.targetOrganizationId,
          },
          {
            isSystemInternal: auth.isSystemInternal,
            callerOrganizationId: auth.callerOrganizationId,
          },
          this.#dbConnection
        );

      if (!readExternalResourcesResult.success)
        throw new Error(readExternalResourcesResult.error);
      if (!readExternalResourcesResult.value)
        throw new Error('Reading externalresources failed');
      if (readExternalResourcesResult.value.length)
        throw new Error(`ExternalResource already exists`);

      if (request.writeToPersistence)
        await this.#externalresourceRepo.insertOne(
          externalresource,
          this.#dbConnection
        );

      return Result.ok(externalresource);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
