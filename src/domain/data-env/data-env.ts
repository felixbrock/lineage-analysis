import { Column } from "../entities/column";
import { Logic} from "../entities/logic";
import { Materialization } from "../entities/materialization";

export interface MatToRemoveRef {
  id: string;
  name: string;
  schemaName: string;
  dbName: string;
}

export interface ColToRemoveRef {
  id: string;
  name: string;
  relationName: string;
  matId: string;
}

export interface LogicToRemoveRef {
  id: string;
  relationName: string;
}

export interface DataEnv {
  matsToCreate?: Materialization[];
  matsToReplace?: Materialization[];
  matToRemoveRefs?: MatToRemoveRef[];
  columnsToCreate?: Column[];
  columnsToReplace?: Column[];
  columnToRemoveRefs?: ColToRemoveRef[];
  logicsToCreate?: Logic[];
  logicsToReplace?: Logic[];
  logicToRemoveRefs?: LogicToRemoveRef[];
}
