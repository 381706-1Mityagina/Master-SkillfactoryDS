# import numpy as np
# import pandas as pd

# df = pd.DataFrame(np.nan, index=[0, 1, 2, 3], columns=['I', 'II', 'III'])
# df.loc[0,"I"] = 1
# df.loc[1,"I"] = 2
# df.loc[2,"I"] = 3
# df.loc[3,"I"] = 4
# df.loc[0,"II"] = 5
# df.loc[1,"II"] = 6
# df.loc[2,"III"] = 7
# df.loc[3,"III"] = 6
# # далее запишите ваш код

# print(df)
# df.index = range(1, 5)
# print(df)
# df.columns = ['A', 'B', 'C']
# print(df)
# df[df.isnull()] = 55
# print(df)

# -----------------------------------------------------------------------------------------
# import pandas as pd
# import numpy as np

# fruit = np.array(["lemons", "lemons", "lemons", "lemons",
#              	"apples", "apples", "apples", "apples",
#              	"apples", "apples", "apples"],
#             	dtype=object)

# shop = np.array(["Shop A", "Shop A", "Shop A", "Shop B",
#              	"Shop A", "Shop A", "Shop A", "Shop B",
#              	"Shop B", "Shop B", "Shop A"],
#             	dtype=object)
 
# pl = np.array(["online", "online", "offline",
#              	"online", "online", "offline",
#              	"offline", "online", "offline",
#              	"offline", "offline"],
#             	dtype=object)

# df = pd.DataFrame({'fruit': fruit, 'shop': shop, 'pl': pl,
#                	"Q": [1, 2, 2, 3, 3, 4, 5, 6, 7, 4, 4],
#                	"P": [5, 4, 5, 5, 6, 6, 8, 9, 9, 3, 3]})
# df['total'] = df['Q']*df['P']
# # print(df)

# subset = df[(df['Q'] > 3) & (df['shop'] == 'Shop A')]
# total2 = subset.iloc[1]['total']
# # print(subset)
# # print(total2)

# fruit_total = df.groupby('fruit')['total'].sum()
# # print(fruit_total)

# fruit_quantity = df.groupby('fruit')['Q'].sum()
# # print(fruit_quantity)

# lemon_average_price = (df[df['fruit'] == 'lemons']).groupby('fruit')['P'].mean()
# print(lemon_average_price)

# -----------------------------------------------------------------------------------------
# import pandas as pd
# import numpy as np

# fruit = np.array(["lemons", "lemons", "lemons", "lemons",
#                  "apples", "apples", "apples", "apples",
#                  "apples", "apples", "apples"],
#                 dtype=object)

# shop = np.array(["Shop A", "Shop A", "Shop A", "Shop B",
#                  "Shop A", "Shop A", "Shop A", "Shop B",
#                  "Shop B", "Shop B", "Shop A"],
#                 dtype=object)
  
# pl = np.array(["online", "online", "offline",
#                  "online", "online", "offline",
#                  "offline", "online", "offline",
#                  "offline", "offline"],
#                 dtype=object)



# df = pd.DataFrame({'fruit': fruit, 'shop': shop, 'pl': pl,
#                    "Q": [1, 2, 2, 3, 3, 4, 5, 6, 7, 4, 4],
#                    "P": [5, 4, 5, 5, 6, 6, 8, 9, 9, 3,3]})
# df['total'] = df['Q']*df['P']
# # далее запишите ваш код
# print(df)

# pivot = pd.pivot_table(df, values='total', index='shop', columns='pl', aggfunc=np.sum)
# print(pivot)
# print(pivot.iloc[1][1])

# -----------------------------------------------------------------------------------------

# import pandas as pd

# def create_medications(names, counts):
#     ser = pd.Series(data = counts, index = names)
#     return ser

# def get_percent(medications, name):
#     return (medications[name] / medications.sum()) * 100

# names = ['chlorhexidine', 'cyntomycin', 'afobazol']
# counts = [15, 18, 7]
# medications = create_medications(names, counts)

# print(get_percent(medications, "chlorhexidine")) #37.5
