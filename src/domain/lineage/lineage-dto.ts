import { Lineage } from '../entities/lineage';

export interface LineageDto {
  lineage: { [key: string]: string }[];
}

export const buildLineageDto = (lineage: Lineage): LineageDto => ({
  lineage: lineage.lineage,
});