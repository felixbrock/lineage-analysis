export interface ColumnProperties {
  id: string;
  name: string;
  tableId: string;
  lineageId: string;
}

export class Column {
  #id: string;

  #name: string;

  #tableId: string;


  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get tableId(): string {
    return this.#tableId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: ColumnProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#tableId = properties.tableId;
    this.#lineageId = properties.lineageId;
  }

  static create(properties: ColumnProperties): Column {
    if (!properties.id) throw new TypeError('Column must have id');
    if (!properties.name) throw new TypeError('Column must have name');
    if (!properties.tableId) throw new TypeError('Column must have tableId');
    if (!properties.lineageId)
      throw new TypeError('Column must have lineage version');

    const column = new Column({
      id: properties.id,
      name: properties.name,
      tableId: properties.tableId,
      lineageId: properties.lineageId,
    });

    return column;
  }
}
