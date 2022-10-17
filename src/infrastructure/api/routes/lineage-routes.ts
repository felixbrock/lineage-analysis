import { Router } from 'express';
import app from '../../ioc-register';
import CreateLineageController from '../controllers/create-lineage-controller';
import ReadLineageController from '../controllers/read-lineage-controller';

const lineageRoutes = Router();

const getAccounts = app.resolve('getAccounts');
const dbo = app.resolve('dbo');

const readLineageController = new ReadLineageController(
  app.resolve('readLineage'),
  getAccounts,
  dbo
);

lineageRoutes.post('/', (req, res) => {
  new CreateLineageController(
    app.resolve('createLineage'),
    getAccounts,
    dbo
  ).execute(req, res);
});

lineageRoutes.get('/:id', (req, res) => {
  readLineageController.execute(req, res);
});

lineageRoutes.get('/', (req, res) => {
  readLineageController.execute(req, res);
});

export default lineageRoutes;
