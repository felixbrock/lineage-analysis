import { Logic } from '../value-types/logic';

interface ModelProperties {
  id: string;
  dbtModelId: string;
  logic: Logic;
  lineageId: string;
}

export interface ModelPrototype {
  id: string;
  dbtModelId: string;
  parsedLogic: string;
  lineageId: string;
}

export class Model {
  #id: string;

  #dbtModelId: string;

  #logic: Logic;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get dbtModelId(): string {
    return this.#dbtModelId;
  }

  get logic(): Logic {
    return this.#logic;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: ModelProperties) {
    this.#id = properties.id;
    this.#dbtModelId = properties.dbtModelId;
    this.#logic = properties.logic;
    this.#lineageId = properties.lineageId;
  }

  static create(prototype: ModelPrototype): Model {
    if (!prototype.id) throw new TypeError('Model must have id');
    if (!prototype.dbtModelId) throw new TypeError('Model must have dbtModelId');
    if (!prototype.parsedLogic)
      throw new TypeError('Model creation requires parsed SQL logic');
    if (!prototype.lineageId) throw new TypeError('Model must have lineageId');

    const logic = Logic.create({ parsedLogic: prototype.parsedLogic });

    const model = new Model({
      id: prototype.id,
      dbtModelId: prototype.dbtModelId,
      logic,
      lineageId: prototype.lineageId,
    });

    return model;
  }
}
