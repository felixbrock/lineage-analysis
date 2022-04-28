export enum DependencyType {
  DATA = 'DATA',
  QUERY = 'QUERY',
  DEFINITION = 'DEFINITION'
}

export interface DependencyProperties {
  id: string,
  type: DependencyType,
  headId: string,
  tailId: string,
  lineageId: string
}

export class Dependency {
  #id: string;

  #type: DependencyType;

  #headId: string;

  #tailId: string;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get type(): DependencyType {
    return this.#type;
  }

  get headId(): string {
    return this.#headId;
  }

  get tailId(): string {
    return this.#tailId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: DependencyProperties) {
    this.#id = properties.id;
    this.#type = properties.type;
    this.#headId = properties.headId;
    this.#tailId = properties.tailId;
    this.#lineageId = properties.lineageId;
  }

  static create(properties: DependencyProperties): Dependency {
    if (!properties.id) throw new TypeError('Dependency object must have id');
    if (!properties.type)
      throw new TypeError('Dependency object must have type');
    if (!properties.headId)
      throw new TypeError('Dependency object must have headId');
    if (!properties.tailId)
      throw new TypeError('Dependency object must have tailId');
      if (!properties.lineageId)
      throw new TypeError('Dependency object must have lineageId');


    const dependency = new Dependency(properties);

    return dependency;
  }
}
