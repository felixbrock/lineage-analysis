import { Column } from "../entities/column";
import { Logic} from "../entities/logic";
import { Materialization } from "../entities/materialization";

export interface DataEnv {
  matsToCreate?: Materialization[];
  matsToReplace?: Materialization[];
  matIdsToRemove?: Materialization[];
  columnsToCreate?: Column[];
  columnsToReplace?: Column[];
  columnIdsToRemove?: Column[];
  logicsToCreate?: Logic[];
  logicsToReplace?: Logic[];
  logicIdsToRemove?: Logic[];
}
