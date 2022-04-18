export enum DependencyType {
  DATA = 'DATA',
  QUERY = 'QUERY',
  DEFINITION = 'DEFINITION'
}

export interface DependencyProperties {
  id: string,
  type: DependencyType,
  headColumnId: string,
  tailColumnId: string,
  lineageId: string
}

export class Dependency {
  #id: string;

  #type: DependencyType;

  #headColumnId: string;

  #tailColumnId: string;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get type(): DependencyType {
    return this.#type;
  }

  get headColumnId(): string {
    return this.#headColumnId;
  }

  get tailColumnId(): string {
    return this.#tailColumnId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: DependencyProperties) {
    this.#id = properties.id;
    this.#type = properties.type;
    this.#headColumnId = properties.headColumnId;
    this.#tailColumnId = properties.tailColumnId;
    this.#lineageId = properties.lineageId;
  }

  static create(properties: DependencyProperties): Dependency {
    if (!properties.id) throw new TypeError('Dependency object must have id');
    if (!properties.type)
      throw new TypeError('Dependency object must have type');
    if (!properties.headColumnId)
      throw new TypeError('Dependency object must have headColumnId');
    if (!properties.tailColumnId)
      throw new TypeError('Dependency object must have tailColumnId');
      if (!properties.lineageId)
      throw new TypeError('Dependency object must have lineageId');


    const dependency = new Dependency(properties);

    return dependency;
  }
}
