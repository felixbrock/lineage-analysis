import { Router } from 'express';
import ReadTableController from '../controllers/read-table-controller';
import app from '../../ioc-register';
import TableDomain from '../../../domain/table-domain';

const tableRoutes = Router();
const tableDomain: TableDomain = app.tableMain;

const readTableController = new ReadTableController(
  tableDomain.readTable,
  app.container.resolve('getAccounts'),
  app.container.resolve('parseSQL')
);

tableRoutes.get('/:tableId', (req, res) =>
  readTableController.execute(req, res)
);

export default tableRoutes;
