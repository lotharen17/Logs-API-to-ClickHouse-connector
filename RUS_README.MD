# :ru: Коннектор Logs API Яндекс.Метрики к ClickHouse :bar_chart:

  Этот коннектор (вообще экстрактор) был разработан **экс-сотрудником Яндекс.Метрики** как пет-прожект в эммиграции. Хотел бы передать приветы всем своим бывшим коллегам. 

  Использованные концепты: 
  - ООП; 
  - Паттерн Фабрика (ну...типа); 
  - Singletone паттерн; 
  - Клин код с комментами и докстрингами; 
  - Hate-driven development. 

---

## :running: Быстрый старт  

  Убедитесь, что у вас стоит **Python 3.10+**, **ClickHouse 25+ version** и есть база данных и табличка визитов/хитов, куда вы хотите записать нужные данные. Опционально - создайте таблицу для лога самого скрипта. 

  Потом:  

  1. Установите зависимости как:  ```bash pip install -r requirements.txt```. 
  2. Заполните конфиги (`.json` файлы в директории `config/folder`). [Описание конфигов](#closed_book-описание-конфиг-файлов).  
  3. Запустите интерпретатором `main.py`.  
  4. Наслаждаться процессом.!  

---

## :green_book: Общее описание

  Скрипт делает запрос к [**Yandex Metrica Logs API**](https://yandex.com/dev/metrika/en/logs/) и записывает данные в **локальный** или **удаленный** [**ClickHouse**](https://clickhouse.com/) инстанс (в последнем случае - по SSH). **В текущей версии поддерживается только авторизация через пару логин+пароль для SSH, keychain и авторизация ключами пока не поддержаны**. 

  Работает одиноково хорошо с обеими видами таблиц:  
  - [**Визитов**](https://yandex.com/dev/Metrica/en/logs/fields/visits)  
  - [**Хитов**](https://yandex.com/dev/Metrica/en/logs/fields/hits)  

Конфигурация осуществляется при помощи аж 3 конфиг файлов. [Описание конфиг-файлов](#closed_book-описание-конфиг-файлов).  

---

## :computer: Автоматизация  
Исполнение скрипта можно автоматизировать с помощью:  
- **Cron процесса**  
- **Apache Airflow**  
- **Любого другого оркестратора/автоматизатора**  

---

## :question: Основная функциональность
1. Экстрактор скачивает данные в формате *tsv* в папку, определяемую в конфиге `global_config.json` (`data/` папка по-умолчанию). Я бы использовал `gzip`, но саппорт Метрики подтвердил, что значение `gzip` в заголовке `accept-encoding`, к сожалению, сейчас ничего не делает на серверах Logs API. Но, может быть, популярность этого скрипта и активное написание писем в саппорт помогут изменить ситуацию. 
2. Затем данные записываются сначала на диск, а потом загружаются в  **локальный/удаленный ClickHouse инстанс**.  
   - Если `ssh` параметр в `ch_credentials.json` задан как `null` или `false`, данные записываются локально.  
   - Если `ssh` суб-JSON задан, SSH туннель устанавливается и данные передаются на удаленный `ClickHouse` инстанс.  
3. После исполнения, в общем случае все загруженные файлы удаляются, очередь запросов очищается от текущего запроса, но это зависит от [`global_config.json`](#global_configjson) настроек. 

---

## :japanese_ogre: Требования для корректной работы  
  Перед запуском скрипта убедитесь, что:

1. У вас есть [**счетчик Яндекс.Метрики**](https://yandex.com/support/metrica/general/creating-counter.html).  
2. У вас есть аккаунт Яндекса с, как минимум, **доступом на просмотр** конкретного счетчика Яндекс.Метрики. [Подробнее](https://yandex.com/support/metrica/general/access.html#guest).  
3. **Yandex OAuth Token** с доступом к Яндекс.Метрике. [Можно сгенерить его по инструкции](https://yandex.com/dev/metrika/en/intro/authorization#get-oauth-token).  
4. **ClickHouse инстанс** установленный:  
   - Локально  
   - Удаленно, но на хосте, доступном по SSH ( авторизация по логин + пароль  - единственно поддерживаемые сейчас). 
   - В Docker контейнере (локально или ремоут)  
   - Если  **ClickHouse инстанс установлен удаленно**, данные для SSH соединения должны быть заданы в файле `ch_credentials.json` (сейчас поддерживается только авторизация по паре логин + пароль).  
5. **База данных и таблиц(а/ы), созданные в ClickHouse**:  
   - На данный момент скрипт работает одновременно только с **одной БД, одной таблицой, и одной сущностью Logs API** (визиты или хиты).
   - Чтобы загружать данные в разные таблицы/бд и из разных источников (для разных счетчиокв, либо сущностей) - склонируйте скрипт и измените конфиги каждого из них, чтоб работали с разными наборами.   
   - Вы можете назвать колонки таблицы своей СУБД в точности как названия параметров Logs API (например: `ym:s:visitID`), либо же дать им кастомные имена (например, `visitID`). В первом случае получите чуть больше гибкости: можете оставить список всех полей для загрузки - и они будут выгружены, но маппинг произойдет только по совпадающим именам колонок и выгруженных параметров. В последнем случае (кастомных имен колонок) - порядок следования колонок `ClickHouse` и порядок следования параметров Logs API должны полностью совпадать, как и их количество. 
   - Рекомендованный движок таблицы: [`ReplacingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree), т.к. в Logs API ни поле `Sign`, ни `ver` не отдаются. 
   - Рекомендуемый первичный ключ (ключ сортировки), для визитов:   

     ```sql
     ORDER BY (visitID, counterUserIDHash, counterID)
     ```
    *(Опустите  `counterID` если используете только один счетчик)*  

   - Рекомендуемый первичный ключ (ключ сортировки) для хитов: 

     ```sql
     ORDER BY (watchID, counterUserIDHash, counterID)
     ```
    *(Опустите `counterID` если используете только один счетчик.)*  
   
   - Скрипт также может записывать данные о собственном исполнении в таблицу лога в `ClickHouse`. Чтобы это произошло - заполните название таблицы для лога в параметр `logTable` в файле [`ch_credentials.json`](#ch_credentialsjson) . Таблица для лога должна быть создана следующим образом (скрипт и сам может её создать): 
    ```sql
    CREATE OR REPLACE TABLE db_name.log_table_name
    (	
        datetime 			DateTime NOT NULL, 
        response 			INT NOT NULL,
        endpoint			String NOT NULL, 
        description			String
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(datetime)
    ORDER BY (datetime, response, endpoint);
    ```
6. **`Python 3.10+`** и соответствующий `pip3`.  
7. Зависимости можно установить из файлика `requirements.txt` с помощью `pip` (или `pip3`): 
    ```bash
    pip install -r requirements.txt
    ``` 
8. Свободное место на диске: до 20 гигабайт, т.к. все данные в текущей реализации сначала выкачиваются с сервера, а затем последовательно записываются в СУБД. 

---

## :closed_book: Описание конфиг-файлов

  Скрипт конфигурируется тремя конфиг-файлами. которые хранятся в папке `configs/`. 

### `api_credentials.json`
  - `token` *(string)*: Ваш Yandex OAuth токен (например, `"token": "d2_eerr534"`). [Как получить](https://yandex.com/dev/metrika/en/intro/authorization#get-oauth-token).  
  - `counter` *(string)*: Ваш счетчик Метрики (например, `"counter": "123456"`). 
  - `date1` *(string, nullable)*:  Дата начала, за которую запрашиваются данные. Можете оставить`null` чтобы автоподставился вчерашний день. 
  - `date2` *(string, nullable)*: Дата конца для выкачки лога. Должна быть не больше, чем вчерашний день (это последний день, за который можно запросить данные). Оставьте `null` чтобы задать вчерашний день. Вы можете задать конкретный `date1`, а `date2` оставить `null` чтобы выкачать данные с заданного числа по вчера (учитывайте, что данных не должно быть больше 10гб, а также данные старше года нельзя запрашивать). Либо задать оба параметра: `date1` и `date2`как `null`, чтобы запрашивать данные только за вчера. Инкрементальные загрузки +- так и делаются.  
  - `fields` *(string)*: Список параметров через комму (для [визитов](https://yandex.com/dev/metrika/en/logs/fields/visits) или [хитов](https://yandex.com/dev/metrika/en/logs/fields/hits)).  
  - `source` *(string)*: `"visits"` для визитов, либо `"hits"` для хитов. 
  - `attribution` *(string)*: по-умолчанию `"LASTSIGN"`, [см. список возможных значений](https://yandex.com/dev/metrika/en/logs/param).

  ***Пример заполненного `api_credentials.json` файла:***
  
  ```json
  {   
    "token":"y0_AgA45454545dfdfkdgkajtqe", 
    "counter": "12345678", 
    "date1": "2025-04-27",
    "date2": null,
    "fields":"ym:s:visitID,ym:s:counterID,ym:s:watchIDs,ym:s:date,ym:s:dateTime,ym:s:dateTimeUTC,ym:s:isNewUser,ym:s:startURL,ym:s:endURL,ym:s:pageViews,ym:s:visitDuration,ym:s:bounce,ym:s:ipAddress,ym:s:regionCountry,ym:s:regionCity,ym:s:regionCountryID,ym:s:regionCityID,ym:s:clientID,ym:s:counterUserIDHash,ym:s:networkType,ym:s:goalsID,ym:s:goalsSerialNumber,ym:s:goalsDateTime,ym:s:goalsPrice,ym:s:goalsOrder,ym:s:goalsCurrency,ym:s:<attribution>TrafficSource,ym:s:<attribution>AdvEngine,ym:s:<attribution>ReferalSource,ym:s:<attribution>SearchEngineRoot,ym:s:<attribution>SearchEngine,ym:s:<attribution>SocialNetwork,ym:s:<attribution>SocialNetworkProfile,ym:s:referer,ym:s:<attribution>DirectClickOrder,ym:s:<attribution>DirectBannerGroup,ym:s:<attribution>DirectClickBanner,ym:s:<attribution>DirectClickOrderName,ym:s:<attribution>ClickBannerGroupName,ym:s:<attribution>DirectClickBannerName,ym:s:<attribution>DirectPhraseOrCond,ym:s:<attribution>DirectPlatformType,ym:s:<attribution>DirectPlatform,ym:s:<attribution>DirectConditionType,ym:s:<attribution>CurrencyID,ym:s:from,ym:s:<attribution>UTMCampaign,ym:s:<attribution>UTMContent,ym:s:<attribution>UTMMedium,ym:s:<attribution>UTMSource,ym:s:<attribution>UTMTerm,ym:s:<attribution>openstatAd,ym:s:<attribution>openstatCampaign,ym:s:<attribution>openstatService,ym:s:<attribution>openstatSource,ym:s:<attribution>hasGCLID,ym:s:<attribution>GCLID,ym:s:browserLanguage,ym:s:browserCountry,ym:s:clientTimeZone,ym:s:deviceCategory,ym:s:mobilePhone,ym:s:mobilePhoneModel,ym:s:operatingSystemRoot,ym:s:operatingSystem,ym:s:browser,ym:s:browserMajorVersion,ym:s:browserMinorVersion,ym:s:browserEngine,ym:s:browserEngineVersion1,ym:s:browserEngineVersion2,ym:s:browserEngineVersion3,ym:s:browserEngineVersion4,ym:s:cookieEnabled,ym:s:javascriptEnabled,ym:s:screenFormat,ym:s:screenColors,ym:s:screenOrientation,ym:s:screenOrientationName,ym:s:screenWidth,ym:s:screenHeight,ym:s:physicalScreenWidth,ym:s:physicalScreenHeight,ym:s:windowClientWidth,ym:s:windowClientHeight,ym:s:purchaseID,ym:s:purchaseDateTime,ym:s:purchaseAffiliation,ym:s:purchaseRevenue,ym:s:purchaseTax,ym:s:purchaseShipping,ym:s:purchaseCoupon,ym:s:purchaseCurrency,ym:s:purchaseProductQuantity,ym:s:eventsProductID,ym:s:eventsProductList,ym:s:eventsProductBrand,ym:s:eventsProductCategory,ym:s:eventsProductCategory1,ym:s:eventsProductCategory2,ym:s:eventsProductCategory3,ym:s:eventsProductCategory4,ym:s:eventsProductCategory5,ym:s:eventsProductVariant,ym:s:eventsProductPosition,ym:s:eventsProductPrice,ym:s:eventsProductCurrency,ym:s:eventsProductCoupon,ym:s:eventsProductQuantity,ym:s:eventsProductEventTime,ym:s:eventsProductType,ym:s:eventsProductDiscount,ym:s:eventsProductName,ym:s:productsPurchaseID,ym:s:productsID,ym:s:productsName,ym:s:productsBrand,ym:s:productsCategory,ym:s:productsCategory1,ym:s:productsCategory2,ym:s:productsCategory3,ym:s:productsCategory4,ym:s:productsCategory5,ym:s:productsVariant,ym:s:productsPosition,ym:s:productsPrice,ym:s:productsCurrency,ym:s:productsCoupon,ym:s:productsQuantity,ym:s:productsList,ym:s:productsEventTime,ym:s:productsDiscount,ym:s:impressionsURL,ym:s:impressionsDateTime,ym:s:impressionsProductID,ym:s:impressionsProductName,ym:s:impressionsProductBrand,ym:s:impressionsProductCategory,ym:s:impressionsProductCategory1,ym:s:impressionsProductCategory2,ym:s:impressionsProductCategory3,ym:s:impressionsProductCategory4,ym:s:impressionsProductCategory5,ym:s:impressionsProductVariant,ym:s:impressionsProductPrice,ym:s:impressionsProductCurrency,ym:s:impressionsProductCoupon,ym:s:impressionsProductList,ym:s:impressionsProductQuantity,ym:s:impressionsProductEventTime,ym:s:impressionsProductDiscount,ym:s:promotionID,ym:s:promotionName,ym:s:promotionCreative,ym:s:promotionPosition,ym:s:promotionCreativeSlot,ym:s:promotionEventTime,ym:s:promotionType,ym:s:offlineCallTalkDuration,ym:s:offlineCallHoldDuration,ym:s:offlineCallMissed,ym:s:offlineCallTag,ym:s:offlineCallFirstTimeCaller,ym:s:offlineCallURL,ym:s:parsedParamsKey1,ym:s:parsedParamsKey2,ym:s:parsedParamsKey3,ym:s:parsedParamsKey4,ym:s:parsedParamsKey5,ym:s:parsedParamsKey6,ym:s:parsedParamsKey7,ym:s:parsedParamsKey8,ym:s:parsedParamsKey9,ym:s:parsedParamsKey10,ym:s:<attribution>RecommendationSystem,ym:s:<attribution>Messenger",
    "source":"visits", 
    "attribution": "last"
  }
  ```

### `ch_credentials.json`
  - `login` *(string)*: `ClickHouse` login.  
  - `password` *(string)*: `ClickHouse` password.  
  - `host` *(string)*: `ClickHouse` host address.  
  - `port` *(integer)*: `ClickHouse` port. Или локальный порт, чтобы привязать к `SSH` порту. Если задать `null`, то порт будет выбран автоматически.
  - `db` *(string)*: `ClickHouse` название БД.   
  - `table` *(string)*: `ClickHouse` название таблицы.  
  - `logTable` *(string)*: `ClickHouse` название таблицы, чтобы вгрузить туда лог самого скрипта. 
  - `ssh` *(null or dictionary)*: Если `null` - данные загружаются локально. Но может быть задан `SSH`-суб-JSON. 

  ***Пример файла `ch_credentials.json` без ssh соединения:***
  ```json
  {
    "login":"default", 
    "password": "",
    "host": "localhost",  
    "port": 8123, 
    "db": "debuging", 
    "table": "visits2",
    "logTable": "extractor_log",
    "ssh": null
  }
  ```  
  Но если вам нужно `SSH` соединение, вместо `null` задайте следующие значения для ключа `SSH`: 
    - `login`: `SSH` username.  
    - `password`: `SSH` password.  
    - `host`: название хоста или `IP`.  
    - `port`: порт связи по SSH (обычно - 22).  
    - `remote_port_bind`: удаленный `ClickHouse` порт `HTTP` соединения (по-умолчанидю -  **`8123`**). [Подробнее](https://clickhouse.com/docs/en/interfaces/http#http-interface). На данный момент поддерживается только HTTP-соединение.  

  ***Example of ssh sub-json:***
  ```json
  "ssh": {
    "login": "your_ssh_username",
    "password": "your_ssh_password",
    "host": "remote_host_address",
    "port": 22,
    "remote_port_bind": 8123
  }
  ``` 

### `global_config.json`

  Содержит следующие настройки: 

  - `log_continuous_path`: Путь до инкрементального лога работы скрипта (содержит данные обо всех запусках скрипта). Директория может не существовать - скрипт попытается её создать. 
  По-умолчанию: `"logs/logs.log"`. 

  - `log_last_run_path`: Путь до лога последнего запуска скрипта. Содержит только данные о последнем запуске.  
  По-умолчанию: `"logs/last_run.log"`.

  - `temporary_data_path`: Папка для хранения скачанных файлов. 
  Имя каждого файла формируется по формуле: `datetime-counterId-source(hits/visits)-part{part_number}.tsv`  
  Пример: `2025-06-10 11:24:25-12345678-visits-part9.tsv`
  По-умолчанию: `"data/"`. 
  - `delete_temp_data`: Boolean. Если `true`, удаляет загруженные файлы после окончания скрипта. Если `false`, хранит для вашего дальнейшего использования. 
  По-умолчанию: `true`.

  - `delete_not_uploaded_to_db_temp_data`: Boolean. Если `true` и `delete_temp_data` тоже `true`, исключает файлы, которые не были успешно записаны в СУБД из списка на удаление. Т.е. они остаются и вы можете вгрузить их в СУБД вручнуюю. 
  По-умолчанию: `true`.

  - `api_strict_db_table_cols_names`: Boolean. Если `true`, делает маппинг колонок и параметров Logs API по именам (а не в порядке следования) — только совпадающие по именам параметры будут записаны в СУБД.  
    Если `false`, скрипт попытается записать данные в порядке следования параметров Logs API в соотв. конфиге. Поэтому порядок и количество параметров/колонок становятся критичными (они должны совпадать: и порядок, и количество). 
  По-умолчанию: `false`.

  - `run_db_table_test`: Boolean. Если `true`, производит простенький тест на то существует ли заднные в ch-конфиге база данных и таблица.   
    - Если `continue_on_columns_test_fail` задана `false` и количество колонок в СУБД не совпадает с кол-вом полей Logs API в `api_credentials.json`, скрипт выдаст ошибку и остановит исполнение.   
    - Если `continue_on_columns_test_fail` задано `true`,скрипт продолжит исполнение, считая, что имена колонок/параметров совпадают и параметр`api_strict_db_table_cols_names` включен (`true`).  
    - Если нужно базы данных не существует, скрипт выдаст ошибку и остановит исполнение. 
  По-умолчанию: `true`.

  - `continue_on_columns_test_fail`: Boolean. Имеет смысл только если `run_db_table_test` задан `true`.  
    Если `true`, даже если количество столбцов в СУБД и параметров в `api_credentials.json` отличаются.  
    Будет работать корректно только если параметр `api_strict_db_table_cols_names` тоже задан `true`.  
  По-умолчанию: `false`.

  - `run_log_table_test`: Boolean. Если `true`, проверяет чтобы таблица для лога в файле `ch_credentials.json` (`logTable` параметр) существовала и содержала ожидаемые колонки. 
    Если `false`, эта проверка пропускается.  
  По-умолчанию: `true`.

  - `create_log_table_on_fail`: Boolean. Если `true`, создает таблицу по значению ключа `logTable` (из файла `ch_credentials.json`), если таблица не существует, либо содержит неожиданные колонки. 
    Если `false`, таблица не создается и лог в неё записан не будет.  
  По-умолчанию: `true`.

  - `continue_on_log_table_creation_fail`: Boolean. Если `true`, продолжает исполнение скриата даже если таблица из ключа `logTable` не существует или содержит некорректные колонки. 
    В этом случае лог просто не будет записан в базу. 
  По-умолчанию: `true`.

  - `clear_api_queue`: Boolean. Если `true`, расчищает очередь запросов Logs API в попытках выделить ресурсы на создание нового лога.  
    Если `false`, существующие уже запросы отсанутся, но новый запрос может не быть создан из-за меньшего числа оставшихся ресурсов.
  По-умолчанию: `true`.

  - `clear_created_logs_request`: Boolean. Если `true`, очищает затем запрос в Logs API на даныне, созданный в рамках текущего запуска скрипта. Это - следование золотому правило "убери после себя". 
  По-умолчанию: `true`.

  - `frequency_api_status_check_sec`: Integer. Определяет как часто (в секундуах) скрипт проверяет приготовился ли запрос на данные и можно ли их выкачивать.
  По-умолчанию: `30`.

  - `api_status_wait_timeout_min`: Integer. Максимальный таймаут (в минутах) для ожидания скриптом приготовления данных Logs API.
  По-умолчанию: `30`.

  - `data_loss_tolerance_perc`: Integer. Задает максимально допустимый порог потери данных в процентах (данные, которые не могут быть загружены с серверов Яндекс.Метрики). Если превышен - будет возвращена ошибка. 
  По-умолчанию: `10`.

  - `bad_data_tolerance_perc`: Integer. Задает максимально допустимый процент данных, которые могут не записаться в СУБД в процентах. Если этот процент превышен - будет возвращена ошибка. 
  По-умолчанию: `15`.

  - `absolute_db_format_errors_tolerance`: Integer. Максимальное количество абсолютных ошибок, которые могут возникнуть при загрузке в СУБД одного файла до того, как будет возвращена ошибка.
  По-умолчанию: `10`.

    **Пример файла `global_config.json`:**
    ```json
    {
      "log_continuous_path": "logs/logs.log",
      "log_last_run_path": "logs/last_run.log",
      "temporary_data_path": "data/",
      "delete_temp_data": true,
      "delete_not_uploaded_to_db_temp_data": true,
      "api_strict_db_table_cols_names": true,
      "run_db_table_test": true,
      "continue_on_columns_test_fail": true,
      "run_log_table_test": true,
      "create_log_table_on_fail": true,
      "continue_on_log_table_creation_fail": true,
      "clear_api_queue": true,
      "clear_created_logs_request": true,
      "frequency_api_status_check_sec": 30,
      "api_status_wait_timeout_min": 30,
      "data_loss_tolerance_perc": 10,
      "bad_data_tolerance_perc": 15,
      "absolute_db_format_errors_tolerance": 10
    }
    ```

---

## :notebook: Описание модулей

  Чтобы получить больше деталей насчет взаимосвязи классов в проекте, см. UML-диаграмму. 

  ### 1. `main.py`
  Находится в корне проекта. Импортирует все нужные модули и делает последовательно все нужные операции через безопасную обертку: устаовление соединений с СУБД, проверки, запрос на создание лога, выгрузку данных, загрузку их в `ClickHouse`, удаление данных. 

  ### 2. `routines_utils.py`
  Находится в папке `utils/` проекта. Содержит 2 кастомных исключения: `DatabaseException` and `FlowException` и целый класс `UtilSet` с набором методов. которые позволяют ускорить разработку, упрощая рутинные операции чтения, записи и перезаписи различных файлов.  

  ### 3. `logger.py`
  Находится в папке `utils/` проекта. Содержит класс `Logger`, который логгирует данные и затем записывает локально на жесткий диск и, опционально, в лог-таблицу `ClickHouse`. 

  ### 4. `database_utils.py`
   Находится в папке `utils/` проекта. Содержит класс `ClickHouseConnector` для простой работы и управления соединением с `ClickHouse`, `SSH` туннелем и методами для совершения запросов в СУБД, а также для записи данных в СУБД, закрытия соединений и т.д.  

  ### 5. `api_methods.py`
  Находится в папке `utils/` проекта. Содержит набор классов-синглотонов, наследуемых от абстрактного класса `AbstractRequest`, каждый из которых делает соответсвующий запрос в [LogsAPI](https://yandex.com/dev/metrika/en/logs/openapi/getLogRequests). 

  ### 6. `wrappers.py`
  
  Находится в папке `utils/` проекта. Содержит класс `MainFlowWrapper` который контролирует исполнение и последовательность исполнение логических частей скрипта. Честно говоря, это не обязательный класс, т.к. это и есть тело программы :new_moon_with_face: и это скорее маркер, что автор очень уж пытался в паттерны :new_moon_with_face: :new_moon_with_face: :new_moon_with_face:. Но он все ещё полезен тем, что большинство блоков там обернуты в исключения и ошибки вызываются обдуманно, после записи нужной информации в лог. 

---

## :minidisc: Описание запросов
  
  В проекте содержится 6 запросов в папке `queries/`. Большинство из них - `DML`-запросы (а конкретно, - `SELECT`) для выполнения поверхостных проверок базы данных и таблиц (и для data-таблицы и для таблицы-лога). 

  ### 1. `query_database.sql`
  `SELECT` запрос для проверки существования базы, заданной как значение ключа`db` параметра в файле [`ch_credentials.json`](#ch_credentialsjson). 

  ### 2. `query_table.sql` 
  `SELECT` запрос чтобы проверить, что таблица - значение ключа `table` в файле [`ch_credentials.json`](#ch_credentialsjson) существует. 

  ### 3. `query_columns.sql`
  `SELECT` запрос для выбора всех имен колонок из п2. Это ключевой пункт поверхостной проверки: затем количество колонок будет сравнено с количеством параметров запроса в файле `api_credentials.json`. Успешное прохождение теста значит, что количество колонок и параметров совпадает.

  ### 4. `query_log_table.sql` 
  `SELECT` запрос, который проверяет, что лог-таблица, значение ключа `logTable` файла [`ch_credentials.json`](#ch_credentialsjson) существует. 

  ### 5. `query_log_table_columns.sql`
  `SELECT` запрос для проверки колонок таблицы для лога самого скрипта на то, соответствуют ли эти колонки ожидаемым. 

  ### 6. `create_log_table.sql`
  DDL запрос (`CREATE`) чтобы создать таблицу для лога самого скрипта. Может и, опредлеенно, перезапишет таблицу с таким же названием в случае её существования, если условия дойдут до исполнения этого запроса (т.е. если таблица с таким названием существует, но там другие колонки). 


---

## :rage2: Possible nonsense configs combinations :new_moon_with_face:. 

  1. Определенно бесполезным является создавать таблицу `ClickHouse` с кастомными названиями и задавать в `global_config.json` параметр `"api_strict_db_table_cols_names"`: `true` т.к. данные могут быть выгружены, но попросту не будут записаны в этом случае в СУБД.  

  2. Также, если у вас кастомные имена колонок и количество колонок не совпадает с количетсво запрашиваемых полей в Logs API, а в `global_config.json` параметры `"api_strict_db_table_cols_names"`: `false` но с  `"run_db_table_test"`: `false` - это означает, что тесты БД будут пропущены, данные будут выгружены с серверов Метрики, но не записаны. 

  3. Аналогично предыдущему, когда `global_config.json` параметры `"run_db_table_test"`: `true` и  `"continue_on_columns_test_fail"`: `true`, т.к. это опять же приведет к загрузке данных из Logs API, но они не смогут быть записаны в СУБД. 

  Список будет пополняться. 
      
     



