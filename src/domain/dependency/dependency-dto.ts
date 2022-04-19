import { DependencyType } from "../entities/dependency";

export interface DependencyDto {
  id: string;
  type: DependencyType,
  headColumnId: string,
  tailColumnId: string,
  lineageId: string
}
