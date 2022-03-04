export interface LineageProperties {
  lineage: {[key: string]: string}[]
}

export class Lineage {
  #lineage: { [key: string]: string }[];

  get lineage(): { [key: string]: string }[] {
    return this.#lineage;
  }

  private constructor(properties: LineageProperties) {
    this.#lineage = properties.lineage;
  }

  static create(properties: LineageProperties): Lineage {    
    const lineage = new Lineage(properties);

    return lineage;
  }
}
