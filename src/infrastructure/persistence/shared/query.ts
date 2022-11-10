export interface ColumnDefinition {
  name: string;
  selectType?: string;
}

const relationPath = 'cito.lineage';

export const getInsertQuery = (
  matName: string,
  columnDefinitions: ColumnDefinition[],
  rows: unknown[]
): string => `
      insert into ${relationPath}.${matName}(${columnDefinitions
  .map((el) => el.name)
  .join(', ')})
      select ${columnDefinitions
        .map((el, index) =>
          el.selectType ? `${el.selectType}($${index + 1})` : `$${index + 1}`
        )
        .join(', ')}
      from values ${rows.join(', ')};
      `;

export const getUpdateQuery = (
  matName: string,
  columnDefinitions: ColumnDefinition[],
  rows: unknown[]
): string => `
        merge into ${relationPath}.${matName} target
        using (
        select ${columnDefinitions
          .map((el, index) =>
            el.selectType
              ? `${el.selectType}($${index + 1}) as ${el.name}`
              : `$${index + 1} as ${el.name}`
          )
          .join(', ')}
        from values ${rows.join(', ')}) as source
        on source.id = target.id
      when matched then update set ${columnDefinitions
        .map((el) => `target.${el.name} = source.${el.name}`)
        .join(', ')};
        `;
const 