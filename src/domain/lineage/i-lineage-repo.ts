import { Lineage, LineageCreationState } from '../entities/lineage';
import { IAuth, IServiceRepo } from '../services/i-service-repo';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

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
    connPool: IConnectionPool,
    targetOrgId?: string
  ): Promise<Lineage | undefined>;
}
