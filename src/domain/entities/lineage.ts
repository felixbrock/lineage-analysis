export interface LineageProperties {
  id: string;
  createdAt?: string;
  organizationId: string;
}

export class Lineage {
  #id: string;

  #createdAt: string;

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get createdAt(): string {
    return this.#createdAt;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: LineageProperties) {
    this.#id = properties.id;
    this.#createdAt = properties.createdAt || new Date().toISOString();
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: LineageProperties): Lineage => {
    if (!properties.id) throw new TypeError('Lineage must have id');
    if (!properties.organizationId) throw new TypeError('Lineage must have organization id');

    const lineage = new Lineage(properties);

    return lineage;
  };
}
