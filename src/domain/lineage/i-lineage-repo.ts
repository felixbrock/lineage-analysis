import { Lineage } from '../entities/lineage';

export interface LineageUpdateDto {
  completed?: boolean;
}

export interface Auth {
  jwt: string;
  callerOrgId: string;
  isSystemInternal: boolean;
}

export interface ILineageRepo {
  findOne(
    lineageId: string,
    targetOrgId: string,
    auth: Auth
  ): Promise<Lineage | null>;
  findLatest(
    filter: { completed: boolean },
    targetOrgId: string,
    auth: Auth
  ): Promise<Lineage | null>;
  all(targetOrgId: string, auth: Auth): Promise<Lineage[]>;
  insertOne(lineage: Lineage, targetOrgId: string, auth: Auth): Promise<string>;
  updateOne(
    lineageId: string,
    updateDto: LineageUpdateDto,
    targetOrgId: string,
    auth: Auth
  ): Promise<string>;
}
