import { Lineage } from '../entities/lineage';

export interface LineageDto {
  id: string;
  createdAt: string;
  organizationId: string
}

export const buildLineageDto = (lineage: Lineage): LineageDto => ({
  id: lineage.id,
  createdAt: lineage.createdAt,
  organizationId: lineage.organizationId
});
