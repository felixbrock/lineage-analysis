export interface DependencyProperties {
  type: string;
  columnId: string;
  direction: string;
}

export enum Direction {
  DOWNSTREAM = 'DOWNSTREAM',
  UPSTREAM = 'UPSTREAM' ,
}

export class Dependency {
  #type: string;

  #columnId: string;

  #direction: string;

  get type(): string {
    return this.#type;
  }

  get columnId(): string {
    return this.#columnId;
  }

  get direction(): string {
    return this.#direction;
  }

  private constructor(properties: DependencyProperties) {
    this.#type = properties.type;
    this.#columnId = properties.columnId;
    this.#direction = properties.direction;
  }

  static create(properties: DependencyProperties): Dependency {
    if (!properties.type)
      throw new TypeError('Dependency object must have type');
    if (!properties.columnId)
      throw new TypeError('Dependency object must have column id');
    if (!properties.direction)
      throw new TypeError('Dependency object must have direction');
    if (!(<any>Object).values(Direction).includes(properties.direction))
      throw new RangeError('Provided dependency direction not valid');

    const dependency = new Dependency(properties);

    return dependency;
  }
}
