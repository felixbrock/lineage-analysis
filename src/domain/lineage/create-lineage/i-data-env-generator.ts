import { Column } from "../../entities/column";
import { Logic, ModelRepresentation } from "../../entities/logic";
import { Materialization } from "../../entities/materialization";
import { ConnectionPool } from "../../snowflake-api/i-snowflake-api-repo";

export interface GenerateResult {
  catalog: ModelRepresentation[];
  materializations: Materialization[];
  columns: Column[];
  logics: Logic[];
}

export interface IDataEnvGenerator {
    generate(connPool: ConnectionPool): Promise<GenerateResult>;
  }
