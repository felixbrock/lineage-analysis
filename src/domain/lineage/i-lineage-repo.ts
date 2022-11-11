import { Lineage } from '../entities/lineage';

export interface LineageUpdateDto {
  completed?: boolean;
}

export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface ILineageRepo {
  findOne(
    lineageId: string,
    auth: Auth,
    targetOrgId?: string,
  ): Promise<Lineage | null>;
  findLatest(
    filter: { completed: boolean },
    auth: Auth,
    targetOrgId?: string,
  ): Promise<Lineage | null>;
  all(
    auth: Auth,
    targetOrgId?: string, 
    ): Promise<Lineage[]>;
  insertOne(lineage: Lineage,
    auth: Auth,
     targetOrgId?: string, 
     ): Promise<string>;
  updateOne(
    lineageId: string,
    updateDto: LineageUpdateDto,
    auth: Auth,
    targetOrgId?: string,
  ): Promise<string>;
}
