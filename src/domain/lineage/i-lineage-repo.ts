import { Lineage, LineageCreationState } from '../entities/lineage';
import { IAuth, IServiceRepo } from '../services/i-service-repo';
import { IDbConnection } from '../services/i-db';

export interface LineageUpdateDto {
  creationState?: LineageCreationState;
  dbCoveredNames?: string[];
  diff?: string;
}

export type LineageQueryDto = undefined;

export interface ILineageRepo
  extends IServiceRepo<Lineage, LineageQueryDto, LineageUpdateDto> {
  findLatest(
    filter: { tolerateIncomplete: boolean; minuteTolerance?: number },
    auth: IAuth,
    dbConnection: IDbConnection,
    targetOrgId?: string
  ): Promise<Lineage | undefined>;
}
