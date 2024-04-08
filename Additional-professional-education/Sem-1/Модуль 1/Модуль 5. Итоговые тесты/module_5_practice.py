import numpy as np

def mnk(x_,y_):
    # запишите далее ваш код  
    A = np.vstack([x_, np.ones(len(x_))]).T
    a, b = np.linalg.lstsq(A, y_, rcond=None)[0]  
    return a, b

#------------------------------------------------------

import pandas as pd
df = pd.read_csv('users.csv', sep=',')

# далее запишите ваш код
# Удалите строки, где есть пропуски.
df = df.dropna()

# Удалите дубликаты, если они имеются в данных.
df = df.drop_duplicates()

# Замените нулевые значения средними по столбцу без учета строки нулевого значения.
for col in df.columns:
    if col == "age":
        mask = df[col] != 0  # Создаем маску, исключая нулевые значения
        col_mean = df.loc[mask, col].mean()  # Вычисляем среднее значение без учета нулевых значений
        df[col] = df[col].mask(df[col] == 0, col_mean)  # Заменяем нулевые значения средними

#------------------------------------------------------

import pandas as pd
Cars = pd.read_csv('Electric_Car.csv')

# далее напишите ваш код
Carsgroupby = Cars.groupby('Brand')['PriceEuro'].mean().reset_index()

#------------------------------------------------------

Cars_speed = Cars.loc[(Cars.PriceEuro > 50000) & (Cars.TopSpeed_KmH > 200) ].reset_index()

#------------------------------------------------------

import pandas as pd
import numpy as np
EC = pd.read_csv('EC.csv', sep=',')
EVP = pd.read_csv('EVP.csv', sep=',')

# далее запишите ваш код
integral = pd.merge(EC, EVP, on='Brand', how='outer')
describe_pd = integral.describe()

