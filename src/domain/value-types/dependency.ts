export interface DependencyProperties {
  type: string;
  columnId: string;
}

export class Dependency {
  #type: string;
  #columnId: string;

  get type(): string {
    return this.#type;
  }

  get columnId(): string {
    return this.#columnId;
  }

  private constructor(properties: DependencyProperties) {
    this.#type = properties.type;
    this.#columnId = properties.columnId;
  }

  static create(properties: DependencyProperties): Dependency {
    if (!properties.type)
      throw new TypeError('Dependency object must have type');
    if (!properties.columnId)
      throw new TypeError('Dependency object must have column id');

    const dependency = new Dependency(properties);

    return dependency;
  }
}
