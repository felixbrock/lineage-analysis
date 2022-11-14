import { Column } from "../../entities/column";
import { Logic, ModelRepresentation } from "../../entities/logic";
import { Materialization } from "../../entities/materialization";

export interface GenerateResult {
  catalog: ModelRepresentation[];
  materializations: Materialization[];
  columns: Column[];
  logics: Logic[];
}

export interface IDataEnvGenerator {
    generate(): Promise<GenerateResult>;
  }
