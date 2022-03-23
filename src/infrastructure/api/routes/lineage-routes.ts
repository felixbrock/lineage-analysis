import { Router } from 'express';
import app from '../../ioc-register';
import LineageDomain from '../../../domain/lineage-domain';
import CreateLineageController from '../controllers/create-lineage-controller';

const lineageRoutes = Router();
const lineageDomain: LineageDomain = app.lineageMain;

const createLineageController = new CreateLineageController(
  lineageDomain.createLineage,
  app.container.resolve('getAccounts'),
);

lineageRoutes.get('/:tableId', (req, res) =>
  createLineageController.execute(req, res)
);

export default lineageRoutes;
