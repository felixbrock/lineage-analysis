import { Lineage } from '../entities/lineage';
import { IAuth, IBaseServiceRepo } from '../services/i-base-service-repo';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

export interface LineageUpdateDto {
  completed?: boolean;
}

export type LineageQueryDto = undefined;

export interface ILineageRepo
  extends IBaseServiceRepo<Lineage, LineageQueryDto, LineageUpdateDto> {
  findLatest(
    filter: { tolerateIncomplete: boolean; minuteTolerance?: number },
    auth: IAuth,
    connPool: IConnectionPool,
    targetOrgId?: string
  ): Promise<Lineage | null>;
}
