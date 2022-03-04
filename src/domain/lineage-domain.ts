import { CreateLineage } from './lineage/create-lineage';

export default class LineageDomain {
  #createLineage: CreateLineage;

  get createLineage(): CreateLineage {
    return this.#createLineage;
  }

  constructor(createLineage: CreateLineage) {
    this.#createLineage = createLineage;
  }
}
