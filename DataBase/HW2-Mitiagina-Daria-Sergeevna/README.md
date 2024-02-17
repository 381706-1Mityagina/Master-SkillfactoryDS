# Master-SkillfactoryDS DataBase homework collection. Задание 2

#### 🌟 Тема домашней работы
Основные операторы PostgreSQL.
#### 🌟 Цель домашней работы
Научиться работать с основными операторами PostgreSQL, фильтровать таблицы по разным условиям, писать вложенные запросы, объединять таблицы.
#### 🌟 Формулировка задания
Дано два csv-файла с данными о клиентах ([customer.csv](https://lms.skillfactory.ru/asset-v1:SkillFactory+MFTIDS+SEP2023+type@asset+block@customer.csv)) и их транзакциях ([transaction.csv](https://lms.skillfactory.ru/asset-v1:SkillFactory+MFTIDS+SEP2023+type@asset+block@transaction.csv)).
Необходимо выполнить следующее:
- Создать таблицы со следующими структурами и загрузить данные из csv-файлов. Детали приведены ниже.
- Выполнить следующие запросы:
   - (1 балл) Вывести все уникальные бренды, у которых стандартная стоимость выше 1500 долларов.
   - (1 балл) Вывести все подтвержденные транзакции за период '2017-04-01' по '2017-04-09' включительно.
   - (1 балл) Вывести все профессии у клиентов из сферы IT или Financial Services, которые начинаются с фразы 'Senior'.
   - (1 балл) Вывести все бренды, которые закупают клиенты, работающие в сфере Financial Services
   - (1 балл) Вывести 10 клиентов, которые оформили онлайн-заказ продукции из брендов 'Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles'.
   - (1 балл) Вывести всех клиентов, у которых нет транзакций.
   - (2 балла) Вывести всех клиентов из IT, у которых транзакции с максимальной стандартной стоимостью.
   - (2 балла) Вывести всех клиентов из сферы IT и Health, у которых есть подтвержденные транзакции за период '2017-07-07' по '2017-07-17'.
#### 🌟Что нужно отправить	
- Cсылку на репозиторий, в котором будут ноутбук в Jupyter с решением или sql-скрипты на PostgreSQL со скринами из DBeaver.

# Процесс/комментарии

Весь процесс работы отображен в [Jupyter notebook - Scripts/HW2-Mitiagina-Daria-Sergeevna.ipynb](https://github.com/381706-1Mityagina/Master-SkillfactoryDS/tree/master/DataBase/HW2-Mitiagina-Daria-Sergeevna/Scripts/HW2-Mitiagina-Daria-Sergeevna.ipynb).

Скриншоты из DBeaver лежат в [папке Diagrams-scrinshots](https://github.com/381706-1Mityagina/Master-SkillfactoryDS/tree/master/DataBase/HW2-Mitiagina-Daria-Sergeevna/Diagrams-scrinshots).
