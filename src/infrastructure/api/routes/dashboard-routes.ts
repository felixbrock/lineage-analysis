import { Router } from 'express';
import app from '../../ioc-register';
import ReadDashbordController from '../controllers/read-dashboards-controller';

const dashboardsRoutes = Router();

const readDashboardsController = new ReadDashbordController(
  app.resolve('readDashboards'),
  app.resolve('getAccounts'),
);

dashboardsRoutes.get('/', (req, res) => {
  readDashboardsController.execute(req, res);
});

export default dashboardsRoutes;
