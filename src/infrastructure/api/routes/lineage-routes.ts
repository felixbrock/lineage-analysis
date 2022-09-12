import { Router } from 'express';
import app from '../../ioc-register';
import ReadLineageController from '../controllers/read-lineage-controller';

const lineageRoutes = Router();

const getAccounts = app.resolve('getAccounts');
const dbo = app.resolve('dbo');

const readLineageController = new ReadLineageController(
  app.resolve('readLineage'),
  getAccounts,
  dbo
);

lineageRoutes.get('/:id', (req, res) => {
  readLineageController.execute(req, res);
});

lineageRoutes.get('/org/:organizationId', (req, res) => {
  readLineageController.execute(req, res);
});

export default lineageRoutes;
