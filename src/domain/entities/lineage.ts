export interface LineageProperties {
  id: string;
  createdAt?: number;
}

export class Lineage {
  #id: string;

  #createdAt: number;

  get id(): string {
    return this.#id;
  }

  get createdAt(): number {
    return this.#createdAt;
  }

  private constructor(properties: LineageProperties) {
    this.#id = properties.id;
    this.#createdAt = properties.createdAt || Date.now();
  }

  static create(properties: LineageProperties): Lineage {
    if (!properties.id) throw new TypeError('Lineage must have id');

    const lineage = new Lineage(properties);

    return lineage;
  }
}
