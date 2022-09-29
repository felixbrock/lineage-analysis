import app from '../../../src/infrastructure/ioc-register';
import fs from 'fs';
import { CreateLogicRequestDto } from '../../../src/domain/logic/create-logic';

/* Run single test: npm test -- create-logic.test.ts */

test('logic creation', async () => {
  console.log('test needs to be updated');
  // const createLogic = app.resolve('createLogic');

  // const modelName = 'W_COLUMNS_D';

  // const parsedLogic = fs.readFileSync(
  //   `C:/Users/felix-pc/Hivedive Limited/Hivedive - Documents/Problem28/Product/Sample1SQLModels/model-snowflake_usage-${modelName}.json`,
  //   'utf-8'
  // );

  // const reqDto: CreateLogicRequestDto = {
  //   modelId: `model.snowflake_usage.${modelName}`,
  //   lineageId: 'test-4',
  //   sql: 'test',
  //   parsedLogic,
  // };

  // const auth = { organizationId: 'todo' };

  // const result = await createLogic.execute(reqDto, auth);

  // console.log(result.error);

  // console.log(result.value.statementRefs);

  // expect(result.success).toBe(true);
});
