import SQLElement from './sql-element';

export type StatementReference = [string, string];

interface LogicProperties {
  parsedLogic: string;
  statementReferences: StatementReference[][];
}

export interface LogicPrototype {
  parsedLogic: string;
}

export class Logic {
  #parsedLogic: string;

  #statementReferences: StatementReference[][];

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementReferences(): StatementReference[][] {
    return this.#statementReferences;
  }

  private constructor(properties: LogicProperties) {
    this.#parsedLogic = properties.parsedLogic;
    this.#statementReferences = properties.statementReferences;
  }

  static #appendPath = (key: string, path: string): string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath;
  };

  static #extractstatementReferences = (
    targetKey: string,
    parsedSQL: { [key: string]: any },
    path = ''
  ): StatementReference[] => {
    let referencePath = path;
    const statementReferencesObj: StatementReference[] = [];

    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];
      const value = parsedSQLElement[1];

      if (key === targetKey)
        statementReferencesObj.push([
          this.#appendPath(key, referencePath),
          value,
        ]);
      else if (key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS)
        referencePath = this.#appendPath(value, referencePath);

      // check if value is dictionary
      if (value.constructor === Object) {
        if (key === SQLElement.WILDCARD_IDENTIFIER) {
          const wildcardElementKeys = Object.keys(value);

          const isDotNoation = [
            SQLElement.WILDCARD_IDENTIFIER_DOT,
              SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER,
              SQLElement.WILDCARD_IDENTIFIER_STAR,
          ].every((wildcardElementKey) => wildcardElementKeys.includes(wildcardElementKey));
          if (isDotNoation)
            statementReferencesObj.push([
              this.#appendPath(key, referencePath),
              `${value[SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER]}${
                value[SQLElement.WILDCARD_IDENTIFIER_DOT]
              }${value[SQLElement.WILDCARD_IDENTIFIER_STAR]}`,
            ]);
          else if (
            wildcardElementKeys.length === 1 &&
            wildcardElementKeys.includes(SQLElement.WILDCARD_IDENTIFIER_STAR)
          )
            statementReferencesObj.push([
              this.#appendPath(key, referencePath),
              value[SQLElement.WILDCARD_IDENTIFIER_STAR],
            ]);
        } else {
          const dependencies = this.#extractstatementReferences(
            targetKey,
            value,
            this.#appendPath(key, referencePath)
          );
          dependencies.forEach((dependencyElement) =>
            statementReferencesObj.push(dependencyElement)
          );
        }
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        if (key === SQLElement.COLUMN_REFERENCE) {
          let valuePath = '';
          let keyPath = '';
          value.forEach((valueElement: { [key: string]: any }) => {
            const dependencies = this.#extractstatementReferences(
              targetKey,
              valueElement,
              this.#appendPath(key, referencePath)
            );
            dependencies.forEach((dependencyElement: [string, string]) => {
              valuePath = this.#appendPath(dependencyElement[1], valuePath);
              [keyPath] = dependencyElement;
            });
          });
          statementReferencesObj.push([keyPath, valuePath]);
        } else {
          value.forEach((valueElement: { [key: string]: any }) => {
            const dependencies = this.#extractstatementReferences(
              targetKey,
              valueElement,
              this.#appendPath(key, referencePath)
            );
            dependencies.forEach((dependencyElement) =>
              statementReferencesObj.push(dependencyElement)
            );
          });
        }
      }
    });

    return statementReferencesObj;
  };

  static #getstatementReferences = (fileObj: any): StatementReference[][] => {
    const statementReferences: StatementReference[][] = [];

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementReferencesObj = this.#extractstatementReferences(
        SQLElement.IDENTIFIER,
        fileObj[SQLElement.STATEMENT]
      );
      statementReferences.push(statementReferencesObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      fileObj
        .filter((statement: any) => SQLElement.STATEMENT in statement)
        .forEach((statement: any) => {
          const statementReferencesObj = this.#extractstatementReferences(
            SQLElement.IDENTIFIER,
            statement[SQLElement.STATEMENT]
          );
          statementReferences.push(statementReferencesObj);
        });
    }

    return statementReferences;
  };

  static create(prototype: LogicPrototype): Logic {
    if (!prototype.parsedLogic)
      throw new TypeError('Logic object must have parsed logic');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementReferences = this.#getstatementReferences(
      parsedLogicObj.file
    );

    const logic = new Logic({
      parsedLogic: prototype.parsedLogic,
      statementReferences,
    });

    return logic;
  }
}
