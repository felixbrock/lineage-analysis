import { Router } from 'express';
import ReadSelectorController from '../controllers/read-selector-controller';
import app from '../../ioc-register';
import SelectorDomain from '../../../domain/selector-domain';

const selectorRoutes = Router();
const selectorDomain: SelectorDomain = app.selectorMain;


const readSelectorController = new ReadSelectorController(
  selectorDomain.readSelector, app.container.resolve('getAccounts')
);

selectorRoutes.get('/:selectorId', (req, res) =>
  readSelectorController.execute(req, res)
);

export default selectorRoutes;
