import { ReadSelector } from './selector/read-selector';

export default class SelectorDomain {

  #readSelector: ReadSelector;

  public get readSelector(): ReadSelector {
    return this.#readSelector;
  }

  public constructor(
    readSelector: ReadSelector,
  ) {
    this.#readSelector = readSelector;
  }
}
