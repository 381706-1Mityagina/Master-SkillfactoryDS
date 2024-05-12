from luigi_hw import *

params = {'dataset_name': 'GSE68849'}
luigi.build([ClearEnviroment(**params)], workers=1, local_scheduler=True)