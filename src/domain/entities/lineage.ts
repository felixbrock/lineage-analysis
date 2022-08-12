export interface LineageProperties {
  id: string;
  createdAt?: number;
  organizationId: string;
}

export class Lineage {
  #id: string;

  #createdAt: number;

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get createdAt(): number {
    return this.#createdAt;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: LineageProperties) {
    this.#id = properties.id;
    this.#createdAt = properties.createdAt || Date.now();
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: LineageProperties): Lineage => {
    if (!properties.id) throw new TypeError('Lineage must have id');
    if (!properties.organizationId) throw new TypeError('Lineage must have organization id');

    const lineage = new Lineage(properties);

    return lineage;
  };
}
