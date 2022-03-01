// import dotenv from 'dotenv';

// dotenv.config();

// // eslint-disable-next-line import/first
// import ExpressApp from './infrastructure/api/express-app';
// // eslint-disable-next-line import/first
// import { appConfig } from './config';

// const expressApp = new ExpressApp(appConfig.express);

// expressApp.start();

import express from 'express';
import { spawn } from 'child_process';

enum SQLElement {
  FILE = 'file',
  STATEMENT = 'statement',

  CREATE_TABLE_STATEMENT = 'create_table_statement',

  JOIN_CLAUSE = 'join_clause',
  ODERBY_CLAUSE = 'orderby_clause',

  SELECT_CLAUSE_ELEMENT = 'select_clause_element',
  FROM_EXPRESSION_ELEMENT = 'from_expression_element',
  JOIN_ON_CONDITION = 'join_on_condition',

  TABLE_REFERENCE = 'table_reference',
  COLUMN_REFERENCE = 'column_reference',

  IDENTIFIER = 'identifier',
}

enum LineageInfo {
  TABLE_SELF = 'table_self',
  TABLE = 'table',

  COLUMN = 'column',

  DEPENDENCY_TYPE = 'dependency_type',

  TYPE_SELECT = 'select',
  TYPE_JOIN_CONDITION = 'join_condition',
  TYPE_ORDERBY_CLAUSE = 'oderby_clause',
}

const app = express();
const port = 3000;

app.get('/', (req, res) => {
  const largeDataSet: any[] = [];

  const python = spawn('python', [
    './src/sql-parser.py',
    'C://Users/felix-pc/Desktop/Test/table2.sql',
    'snowflake',
  ]);

  python.stdout.on('data', (data) => {
    console.log('Pipe data from python script ...');
    largeDataSet.push(data.toString());
  });

  python.on('close', (code) => {
    console.log(`child process close all stdio with code ${code}`);

    res.send(largeDataSet.join(''));
  });
});
app.listen(port, () =>
  console.log(`Example app listening on port 
  ${port}!`)
);
