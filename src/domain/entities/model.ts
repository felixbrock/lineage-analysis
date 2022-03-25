import { ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { ParsedSQLDto } from '../sql-parser-api/parsed-sql-dto';
import { Logic } from '../value-types/logic';

interface ModelProperties {
  id: string;
  location: string;
  logic: Logic;
  lineageId: string;
}

export interface ModelPrototype {
  id: string;
  location: string;
  parsedLogic: string;
  lineageId: string;
}

export class Model {
  #id: string;

  #location: string;

  #logic: Logic;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get location(): string {
    return this.#location;
  }

  get logic(): Logic {
    return this.#logic;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: ModelProperties) {
    this.#id = properties.id;
    this.#location = properties.location;
    this.#logic = properties.logic;
    this.#lineageId = properties.lineageId;
  }

  static create(prototype: ModelPrototype): Model {
    if (!prototype.id) throw new TypeError('Model must have id');
    if (!prototype.location) throw new TypeError('Model must have location');
    if (!prototype.parsedLogic)
      throw new TypeError('Model creation requires parsed SQL logic');
    if (!prototype.lineageId) throw new TypeError('Model must have lineageId');

    const logic = Logic.create({ parsedLogic: prototype.parsedLogic });

    const model = new Model({
      id: prototype.id,
      location: prototype.location,
      logic,
      lineageId: prototype.lineageId,
    });

    return model;
  }
}
