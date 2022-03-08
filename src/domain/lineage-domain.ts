import { CreateLineage } from './column/create-column';

export default class LineageDomain {
  #createLineage: CreateLineage;

  get createLineage(): CreateLineage {
    return this.#createLineage;
  }

  constructor(createLineage: CreateLineage) {
    this.#createLineage = createLineage;
  }
}
