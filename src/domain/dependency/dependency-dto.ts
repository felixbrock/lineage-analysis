import { DependencyType } from "../entities/dependency";

export interface DependencyDto {
  id: string;
  type: DependencyType,
  headId: string,
  tailId: string,
  lineageId: string
}
