import pandas as pd

def retention_game (df_reg, df_auth, date_reg_1, date_reg_2, ret_1, ret_2):
    """" Возвращает retention заданых дней, для заданных когорт ( Когорты определены по дню регистрации).
    :param df_reg: Датасет с информацией о регистрации пользователей
    :type df_reg: columns [reg_ts] в формате unix time - int и [uid] - id  пользователя в int
    :param df_auth: Датасет с информацией о авторизациях пользователей
    :type df_reg: columns [auth_ts] в формате unix time - int и [uid] - id  пользователя в int
    :param date_reg_1: Дата первой когорты, из промежутка который нас интересуте
    :type date_reg_1: Формат ввода: "YYYY-MM-DD" str   
    :param date_reg_2: Дата последней когорты, из промежутка который нас интересуте
    :type date_reg_2: Формат ввода: "YYYY-MM-DD" str
    :param ret_1: Указать Retention какого дня нас интересует
            ( если только за один день, то продублировать значение в ret_2, если инетересует интервал то указать
            день начала интервала)
    :type ret_1:  int
    :param ret_2: Указать Retention какого дня нас интересует
            ( если только за один день, то продублировать значение из ret_1, если инетересует интервал то указать
            день конца интервала)
    :type ret_2:  int
                
    """    
    # Переводим столбцы из unit time в формат "YYYY-MM-DD"    
    df_reg['reg_ts'] = pd.to_datetime(df_reg.reg_ts, unit='s')
    df_reg.reg_ts    = df_reg.reg_ts.dt.normalize()
    df_auth.auth_ts  = pd.to_datetime(df_auth.auth_ts, unit='s')
    df_auth.auth_ts  = df_auth.auth_ts.dt.normalize()
    # Объеденяем два датасета
    df_auth_reg      = df_auth.merge(df_reg, on = 'uid', how = 'left')
    # Создаем столбец с информаей о разнице ней между регистрацией и авторизацией
    df_auth_reg['diff'] = df_auth_reg['auth_ts'] - df_auth_reg['reg_ts'] 
    # Создаем датасет где индексы - даты регистрации, столбцы - количество дней с регистрации до авторизации
    df_pivot = df_auth_reg \
        .groupby(['reg_ts','diff'], as_index = False) \
        .agg({'uid':'nunique'}) \
        .pivot(index='reg_ts', columns='diff', values='uid')
    # Высчитываем Retention в процентах и округляем до 1 значения после запятой
    df_retention = df_pivot.divide(df_pivot['0 days'],axis=0).mul(100).fillna(0).round(1)
    day_1 = f'{ret_1} days'
    day_2 = f'{ret_2} days'
    # Выбираем нужные нам данные, на основание заданных параметров 
    return df_retention.loc[date_reg_1:date_reg_2,day_1:day_2]
