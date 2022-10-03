import {
  ExternalResource,
  ExternalResourceType,
} from '../entities/external-resource';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import {
  IExternalResourceRepo,
  ExternalResourceQueryDto,
} from './i-external-resource-repo';

export interface ReadExternalResourcesRequestDto {
  name?: string;
  type?: ExternalResourceType;
  lineageId: string;
  targetOrganizationId?: string;
}

export interface ReadExternalResourcesAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type ReadExternalResourcesResponseDto = Result<ExternalResource[]>;

export class ReadExternalResources
  implements
    IUseCase<
      ReadExternalResourcesRequestDto,
      ReadExternalResourcesResponseDto,
      ReadExternalResourcesAuthDto,
      DbConnection
    >
{
  readonly #externalresourceRepo: IExternalResourceRepo;

  #dbConnection: DbConnection;

  constructor(externalresourceRepo: IExternalResourceRepo) {
    this.#externalresourceRepo = externalresourceRepo;
  }

  async execute(
    request: ReadExternalResourcesRequestDto,
    auth: ReadExternalResourcesAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadExternalResourcesResponseDto> {
    try {
      this.#dbConnection = dbConnection;

      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organizationId allocation');

      const externalresources: ExternalResource[] =
        await this.#externalresourceRepo.findBy(
          this.#buildExternalResourceQueryDto(request, organizationId),
          dbConnection
        );
      if (!externalresources)
        throw new ReferenceError(`Queried externalresources do not exist`);

      return Result.ok(externalresources);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildExternalResourceQueryDto = (
    request: ReadExternalResourcesRequestDto,
    organizationId: string
  ): ExternalResourceQueryDto => {
    const queryDto: ExternalResourceQueryDto = {
      lineageId: request.lineageId,
      organizationId,
    };

    if (request.name) queryDto.name = request.name;
    if (request.type) queryDto.type = request.type;

    return queryDto;
  };
}
