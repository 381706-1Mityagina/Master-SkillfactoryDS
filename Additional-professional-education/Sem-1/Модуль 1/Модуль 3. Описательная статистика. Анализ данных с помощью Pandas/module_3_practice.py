# начальный код

import random
import pandas as pd

random.seed(10)

list_metrics = []

for i in range(0,300):
  n = random.randint(-100,1000)
  list_metrics.append(n)

list_metrics = pd.DataFrame({'Прирост': list_metrics})
list_metrics

# далее будет код шаблона
# 1. Рассчитайте стандартное отклонение прироста подписчиков.
result1 = round(list_metrics['Прирост'].std(), 2)
# 2. Рассчитайте размах прироста подписчиков. 
result2 = round(float(list_metrics['Прирост'].max() - list_metrics['Прирост'].min()), 2)

hist = list_metrics.hist()
figure = hist.get_figure()

#---------------------------------------------------------------------------------------------
import pandas as pd
police = pd.read_csv('police.csv', sep=',')

# далее запишите ваш код
police.drop(["county_name"], axis='columns', inplace=True)

#---------------------------------------------------------------------------------------------

police['driver_gender'].value_counts(normalize=True)

#---------------------------------------------------------------------------------------------
