export interface LineageProperties {
  id: string;
  lineage: {[key: string]: any}[]
}

export class Lineage {
  #id: string; 

  #lineage: { [key: string]: string }[];

  get lineage(): { [key: string]: string }[] {
    return this.#lineage;
  }

  get id(): string {
    return this.#id;
  }

  private constructor(properties: LineageProperties) {
    this.#lineage = properties.lineage;
    this.#id = properties.id;
  }

  static create(properties: LineageProperties): Lineage {    
    if (!properties.id) throw new TypeError('Lineage must have id');

    const lineage = new Lineage(properties);

    return lineage;
  }
}
