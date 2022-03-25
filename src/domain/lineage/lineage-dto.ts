import { Lineage } from '../entities/lineage';

export interface LineageDto {
  id: string;
  createdAt: number;
}

export const buildLineageDto = (lineage: Lineage): LineageDto => ({
  id: lineage.id,
  createdAt: lineage.createdAt,
});
