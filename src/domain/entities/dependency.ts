export enum DependencyType {
  DATA = 'DATA',
  QUERY = 'QUERY',
  DEFINITION = 'DEFINITION',
  EXTERNAL = 'EXTERNAL',
}

export interface DependencyProperties {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
  lineageId: string;
  organizationId: string;
}

export class Dependency {
  #id: string;

  #type: DependencyType;

  #headId: string;

  #tailId: string;

  #lineageId: string;

  #organizationId: string;

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

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: DependencyProperties) {
    this.#id = properties.id;
    this.#type = properties.type;
    this.#headId = properties.headId;
    this.#tailId = properties.tailId;
    this.#lineageId = properties.lineageId;
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: DependencyProperties): Dependency => {
    if (!properties.id) throw new TypeError('Dependency object must have id');
    if (!properties.type)
      throw new TypeError('Dependency object must have type');
    if (!properties.headId)
      throw new TypeError('Dependency object must have headId');
    if (!properties.tailId)
      throw new TypeError('Dependency object must have tailId');
    if (!properties.lineageId)
      throw new TypeError('Dependency object must have lineageId');
    if (!properties.organizationId)
      throw new TypeError('Dependency object must have oragnizationId');

    const dependency = new Dependency(properties);

    return dependency;
  };
}
