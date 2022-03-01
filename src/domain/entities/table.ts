import { DependencyAnalyzer } from '../value-types/dependency-analyzer';

export interface TableProperties {
  id: string;
  name: string;
  columns: string[];
  parents: Table[];
  statementDependencies: [string, string][];
  lineageInfo: { [key: string]: string }[];
}

export class Table extends DependencyAnalyzer {
  #id: string;

  #name: string;

  #columns: string[];

  #parents: Table[];

  #statementDependencies: [string, string][];

  #lineageInfo: { [key: string]: string }[];

  public get name(): string {
    return this.#name;
  }

  public get columns(): string[] {
    return this.#columns;
  }

  public get statementDependencies(): [string, string][] {
    return this.#statementDependencies;
  }

  public get lineageInfo(): { [key: string]: string }[] {
    return this.#lineageInfo;
  }

  // public get id(): string {
  //   return this.#id;
  // }

  // public set content(content: string) {
  //   if (!content) throw new Error('Selector must have content');

  //   this.#content = content;
  // }

  private constructor(properties: TableProperties) {
    super();
    this.#id = properties.id;
    this.#name = properties.name;
    this.#columns = properties.columns;
    this.#parents = properties.parents;
    this.#statementDependencies = properties.statementDependencies;
    this.#lineageInfo = properties.lineageInfo;
  }

  public static create(id: string): Table {
    if (!id) throw new Error('Table must have id');

    // TODO - populate

    const properties: TableProperties = {id, }

    const table = new Table(properties);
    return table;
  }
}
