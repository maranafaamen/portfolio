# Импортируем модули и библиотеки

from datetime import date
from itertools import product
from matplotlib.patheffects import withStroke

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Включаем отображение всех столбцов

pd.set_option('display.max_columns', None)

# Создаем датафреймы из файлов выгрузки

payments_df = pd.read_csv('db_data/payments.csv')

persents_df = pd.read_csv('db_data/persents.csv', index_col=0)

registrations_df = pd.read_csv('db_data/registrations.csv', index_col=0)

budget_df = pd.read_excel('db_data/budget_jan_2021.xlsx')

# Удаляем дубликаты из датафреймов

payments_df = payments_df.drop_duplicates()

registrations_df = registrations_df.drop_duplicates() 

# Задание 1. Нахождение оптимального срока оценки окупаемости РК

# Джоиним датафреймы и фильтруем столбцы с пустым значением campaign

payments_adv_df = pd.merge(
    pd.merge(payments_df, persents_df, on='Payment_types'),
    registrations_df,
    on='account_id'
)
payments_adv_df.dropna(subset=['campaign'])

# Создаем столбец gross с прибылью с транзакции за вычетом комиссии

payments_adv_df['gross_profit'] = payments_adv_df['real_cost'] * (1 - payments_adv_df['real_cost'] / 100)

# Преобразуем столбцы с датами к соответствующему формату

payments_adv_df['payment_date'] = pd.to_datetime(payments_adv_df['payment_date'])
payments_adv_df['payment_date'] = payments_adv_df['payment_date'].dt.date

# Группируем значения по дате и рекламной кампании

grouped_payments_df = payments_adv_df.groupby(['campaign', 'payment_date'])['gross_profit'].sum().reset_index()

# Создаем столбец с кумулятивной суммой прибыли на каждый день:

grouped_payments_df['cumm_profit'] = grouped_payments_df.groupby('campaign')['gross_profit'].cumsum()

# Сформируем столбец в датафрейме budget_df с названиями рекламных кампаний

budget_df['media_source'].fillna(method='ffill', inplace=True)
budget_df['campaign'] = (
        budget_df['Target'].str[:3].str.upper() +
        "_MS" +
        budget_df['media_source'].str[-1] +
        "_" +
        budget_df['Campaign_type']
)

# Джоиним таблицу grouped_payments_df и столбец с тратами за месяц из budget_df

cost_and_payments_df = pd.merge(
    grouped_payments_df,
    budget_df[['campaign', 'Spend, USD']].head(-1),
    on='campaign',
    how='right'
)

# Создадим отдельный столбец для расчета накопительного ROMI, %

cost_and_payments_df['ROMI, %'] = cost_and_payments_df['cumm_profit'] * 100 / cost_and_payments_df['Spend, USD']

# Отфильтруем уникальные названия кампаний для создания отдельных линий на графике

campaign_names = cost_and_payments_df['campaign'].unique()

# Создадим датафрейм со всеми датами за 2021 год для каждой кампании, для более корректного построения графика ROMI

all_dates_df = pd.DataFrame(
    list(product(campaign_names, pd.date_range(start='2021-01-01', end='2021-12-31').date)),
    columns=['campaign', 'date']
)

# Джоиним его с cost_and_payments_df

cost_and_payments_df = pd.merge(
    cost_and_payments_df,
    all_dates_df,
    left_on=['campaign', 'payment_date'],
    right_on=['campaign', 'date'],
    how='right'
)

# Заполняем пропущенные значения ROMI предыдущими данными

cost_and_payments_df['ROMI, %'] = cost_and_payments_df.groupby('campaign')['ROMI, %'].fillna(method='ffill')

# Сглаживаем значения ROMI для каждой кампании

cost_and_payments_df['ROMI, %'] = (
    cost_and_payments_df.groupby('campaign')['ROMI, %']
    .rolling(window=50)
    .median()
    .reset_index(level=0, drop=True)
)

# Строим графики зависимости ROMI от времени (в днях)

plt.figure(figsize=(10, 5))

for name in campaign_names:
    if name[8:] == 'purchase':
        campaign_data = cost_and_payments_df[cost_and_payments_df['campaign'] == name]
        plt.plot(campaign_data['date'], campaign_data['ROMI, %'], label=name)

# Добавляем для наглядности горизонтальную прямую для ROMI = 100% - "Точка безубыточности"

plt.axhline(y=100, color='r', linestyle='--', label='Точка безубыточности')

plt.title('График ROMI по рекламным кампаниям "purchase"')
plt.xlabel('Дата')
plt.ylabel('ROMI, %')
plt.legend()

# Строим графики производных зависимости ROMI от времени (в днях)

plt.figure(figsize=(10, 5))


for name in campaign_names:
    if name[8:] == 'purchase':
        campaign_data = cost_and_payments_df[cost_and_payments_df['campaign'] == name]
        timeseries_df = campaign_data[['date', 'ROMI, %']] # Создаем временной ряд для нахождения производной
        timeseries_df.set_index('date', inplace=True)
        plt.plot(timeseries_df['ROMI, %'].diff().dropna(), label=f'Производная {name}')

plt.title('График производной ROMI по рекламным кампаниям "purchase"')
plt.xlabel('Дата')
plt.ylabel('d(ROMI)/dt')
plt.legend()


# Строим графики зависимости ROMI от времени (в днях)

plt.figure(figsize=(10, 5))

for name in campaign_names:
    if name[8:] == 'install':
        campaign_data = cost_and_payments_df[cost_and_payments_df['campaign'] == name]
        plt.plot(campaign_data['date'], campaign_data['ROMI, %'], label=name)

# Добавляем для наглядности горизонтальную прямую для ROMI = 100% - "Точка безубыточности"

plt.axhline(y=100, color='r', linestyle='--', label='Точка безубыточности')

plt.title('График ROMI по рекламным кампаниям "install"')
plt.xlabel('Дата')
plt.ylabel('ROMI, %')
plt.legend()

# Строим графики производных зависимости ROMI от времени (в днях)

plt.figure(figsize=(10, 5))

for name in campaign_names:
    if name[8:] == 'install':
        campaign_data = cost_and_payments_df[cost_and_payments_df['campaign'] == name]
        timeseries_df = campaign_data[['date', 'ROMI, %']] # Создаем временной ряд для нахождения производной
        timeseries_df.set_index('date', inplace=True)
        plt.plot(timeseries_df['ROMI, %'].diff().dropna(), label=f'Производная {name}')

plt.title('График производной ROMI по рекламным кампаниям "install"')
plt.xlabel('Дата')
plt.ylabel('d(ROMI)/dt')
plt.legend()

# Задание 2. Оценка успешности РК

# Покупки

# Фильтруем данные о кампаниях по типу кампании

purchases_camps_df = budget_df[['campaign', 'Installs', 'Spend, USD']][budget_df['campaign'].str[8:] == 'purchase']

# Объединяем данные из таблиц

purchases_camps_df = pd.merge(
    purchases_camps_df,
    registrations_df.groupby('campaign')['account_id'].size(),
    on='campaign',
    how='left'
)
purchases_camps_df.rename(columns={'account_id': 'registrations'}, inplace=True)

purchases_camps_df = pd.merge(
    purchases_camps_df,
    payments_adv_df[payments_adv_df['payment_date'] < date(2021,7, 15)].groupby('campaign')['account_id'].nunique(),
    on='campaign',
    how='left'
)
purchases_camps_df.rename(columns={'account_id': 'paying_users'}, inplace=True)

purchases_camps_df = pd.merge(
    purchases_camps_df,
    payments_adv_df[payments_adv_df['payment_date'] < date(2021,7, 15)].groupby('campaign')['gross_profit'].sum(),
    on='campaign',
    how='left'
)

purchases_camps_df = pd.merge(
    purchases_camps_df,
    payments_adv_df[payments_adv_df['payment_date'] < date(2021,7, 15)].groupby('campaign')['real_cost'].sum(),
    on='campaign',
    how='left'
)
purchases_camps_df.rename(columns={'real_cost': 'revenue'}, inplace=True)

purchases_camps_df = pd.merge(
    purchases_camps_df,
    payments_adv_df[payments_adv_df['payment_date'] < date(2021,7, 15)].groupby('campaign')['real_cost'].size(),
    on='campaign',
    how='left'
)
purchases_camps_df.rename(columns={'real_cost': 'purchases'}, inplace=True)


# Рассчитываем метрики в новых столбцах

purchases_camps_df['CAC, USD'] = purchases_camps_df['Spend, USD'] / purchases_camps_df['paying_users']
purchases_camps_df['CR, %'] = round(purchases_camps_df['paying_users'] * 100 / purchases_camps_df['Installs'], 2)
purchases_camps_df['ARPPU, USD'] = round(purchases_camps_df['revenue'] / purchases_camps_df['paying_users'], 3)
purchases_camps_df['avg_purch'] = round(purchases_camps_df['purchases'] / purchases_camps_df['paying_users'], 2)
purchases_camps_df['ROMI, %'] = round(purchases_camps_df['gross_profit'] * 100 / purchases_camps_df['Spend, USD'], 2)

# # Выводим только интересующие нас столбцы

purchases_camps_df.set_index('campaign', inplace=True)

purchases_camps_df = purchases_camps_df[['CAC, USD', 'CR, %', 'avg_purch', 'ARPPU, USD', 'ROMI, %']]

print(purchases_camps_df)

# Установки

# Фильтруем данные о кампаниях по типу кампании

install_camps_df = budget_df[['campaign', 'Installs', 'Spend, USD']][budget_df['campaign'].str[8:] == 'install']

# Объединяем данные из таблиц

install_camps_df = pd.merge(
    install_camps_df,
    registrations_df.groupby('campaign')['account_id'].size(),
    on='campaign',
    how='left'
)
install_camps_df.rename(columns={'account_id': 'registrations'}, inplace=True)

install_camps_df = pd.merge(
    install_camps_df,
    payments_adv_df[payments_adv_df['payment_date'] < date(2021, 8, 1)].groupby('campaign')['gross_profit'].sum(),
    on='campaign',
    how='left'
)

install_camps_df = pd.merge(
    install_camps_df,
    payments_adv_df[payments_adv_df['payment_date'] < date(2021, 8, 1)].groupby('campaign')['real_cost'].sum(),
    on='campaign',
    how='left'
)

install_camps_df.rename(columns={'real_cost': 'revenue'}, inplace=True)

# Рассчитываем метрики в новых столбцах

install_camps_df['CPI, USD'] = round(install_camps_df['Spend, USD'] / install_camps_df['Installs'], 5)
install_camps_df['ARPU, USD'] = round(install_camps_df['revenue'] / install_camps_df['registrations'], 3)
install_camps_df['ROMI, %'] = round(install_camps_df['gross_profit'] * 100 / install_camps_df['Spend, USD'], 2)

# Выводим только интересующие нас столбцы

install_camps_df.set_index('campaign', inplace=True)

install_camps_df = install_camps_df[['Installs', 'CPI, USD', 'ARPU, USD', 'ROMI, %']]

print(install_camps_df)

# Задание 3. Расчет накопительного ARPU

# Выбираем из сгруппированной таблицы данные только по интересующим нас кампаниям

payments_adv_df = payments_adv_df[payments_adv_df['campaign'].str[8:] == 'purchase']

# Находим время жизни пользователя на момент транзакции
 
payments_adv_df['lifetime_day'] = ((
    pd.to_datetime(payments_adv_df['payment_date'])
    - pd.to_datetime(payments_adv_df['created_date']))
    .dt
    .days
    )

# Отсеиваем записи о покупках, которые были совершены до регистрации пользователя (ошибка при генерации данных?) 

payments_adv_df = payments_adv_df[payments_adv_df['lifetime_day'] >= 0]

# Находим revenue для каждой кампании и дня жизни

daily_revenue = (payments_adv_df
                 .groupby(['campaign','created_date','lifetime_day'])
                 ['real_cost'].sum()
                 .reset_index())

# Находим кумулятивную сумму уникальных пользователей для каждой кампании и дня жизни

cumulative_users = (payments_adv_df
                    .groupby(['campaign', 'lifetime_day'])
                    .agg({'account_id': pd.Series.nunique})
                    .groupby(level=0)
                    .cumsum()
                    .reset_index()
                    )

cumulative_users_all = (registrations_df
                     .groupby(['campaign', 'created_date'])
                     ['account_id'].nunique()
                     .groupby(level=0)
                     .cumsum()
                     .reset_index()
                     )
cumulative_users_all.rename(columns={'account_id': 'users_cnt'}, inplace=True)

# Соединяем таблицы

daily_data = pd.merge(daily_revenue, cumulative_users_all, on=['campaign', 'created_date'])

# Рассчитываем APRU для каждого дня жизни
    
daily_data['daily_ARPU'] = daily_data['real_cost'] / daily_data['users_cnt']

# Находим кумулятивную сумму ARPU

daily_data = daily_data.groupby(['campaign', 'lifetime_day'])['daily_ARPU'].sum().groupby(level=0).cumsum().reset_index()
daily_data.rename(columns={'daily_ARPU': 'cumulative_ARPU'}, inplace = True)

# Для упрощения будущих расчетов и экономии времени, напишем функцию, которая 
# возвращает значение накопленного ARPU на n-ный день жизни пользователя

def arpu_n_day(df, day): 
    res = df[df['lifetime_day'] <= day].groupby('campaign')['cumulative_ARPU'].max().reset_index()
    res['day'] = day
    return res
    res = df[df['lifetime_day'] <= day].groupby('campaign')['cumulative_ARPU'].max().reset_index()
    res['day'] = day
    return res

# Cоздаем датафрейм с только интересующими нас днями (7, 14, 30)

arpu_df = pd.concat([arpu_n_day(daily_data, 7), arpu_n_day(daily_data, 14), arpu_n_day(daily_data, 30)], ignore_index=True)

# Строим графики

plt.figure(figsize=(14, 7))

for campaign in arpu_df['campaign'].unique():
    campaign_data = arpu_df[arpu_df['campaign'] == campaign]
    plt.plot(campaign_data['day'], campaign_data['cumulative_ARPU'], marker='o', label=campaign)

# Добавляем аннотации

for line in plt.gca().get_lines():
    for x_value, y_value in zip(line.get_xdata(), line.get_ydata()):
        label = f"{y_value:.2f}"
        plt.annotate(label,
                      (x_value, y_value),
                      textcoords="offset points",
                      ha='center',
                      xytext = (0, 10),
                      color = line.get_color(),
                      path_effects=[withStroke(linewidth=3, foreground="white")])

plt.xlabel('День жизни пользователя')
plt.ylabel('Накопительный ARPU, USD')
plt.title('Изменение накопительного ARPU по кампаниям')
plt.legend(title='Кампания')
plt.xticks(arpu_df['day'].unique())
plt.subplots_adjust(left=0.1, right=0.9, top=1.1, bottom=0.1)


# Задание 4. Найти CPI всех рекламных кампаний

# Считаем фактический CPI

budget_df['CPI, USD'] = budget_df['Spend, USD'] / budget_df['Installs']

cpi_df = budget_df[['campaign', 'CPI, USD']].head(-1)

# Строим барчарт

plt.figure(figsize=(10, 8))

barplot = sns.barplot(x='CPI, USD', y='campaign', data=cpi_df.sort_values('CPI, USD', ascending=True),
            palette=sns.color_palette("coolwarm", len(cpi_df)))

# Добавляем аннотации

for p in barplot.patches:
    barplot.annotate(format(p.get_width(), '.2f'),
                     (p.get_width(), p.get_y() + p.get_height() / 2),
                     ha = 'left', va = 'center',
                     xytext = (5, 0),
                     textcoords = 'offset points')

plt.xlabel('CPI, USD')
plt.ylabel('Кампания')
plt.title('CPI по кампаниям')
plt.tight_layout()

plt.show()
