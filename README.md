# Extractor + Connector for Yandex.Metrica Logs API to Local ClickHouse (OOP Version)  

---

## Quick Start  

Ensure you have **Python 3.10+**, **ClickHouse 25+ version** installed, and a database with at least one table created.  
Then:  
0. Install all dependencies by running  ```bash pip install -r requirements.txt```. 
1. Fill in the configuration files (`.json` files in config/folder). [See more details below](#config-files).  
2. Run `main.py`.  
3. Enjoy your Logs API data in ClickHouse!  

---

## General Description  

  This extractor was developed by ex-Yandex.Metrika employee as a pet project to pull data from the [**Yandex Metrica Logs API**](https://yandex.com/dev/Metrica/en/logs/) and store it in a **local** or **remote** [**ClickHouse**](https://clickhouse.com/) instance via SSH. **Currently supported only HTTP interface and SSH tunneling with login + password authorization, no keychains support yet**. 

  It works seamlessly with both:  
  - [**Sessions/Visits Table**](https://yandex.com/dev/Metrica/en/logs/fields/visits)  
  - [**Events/Hits Table**](https://yandex.com/dev/Metrica/en/logs/fields/hits)  

Configuration is determined via multiple JSON files. [See the config files section](#config-files).  

### Automation  
Extractor execution can be automated using:  
- **Cron Jobs**  
- **Apache Airflow**  
- **Any Other Orchestration Tool**  

### What it basically does?
1. Extractor downloads data in **TSV format** in folder, determined by global_config.json file. It would have used gzip, but, unfortunatelly,there is confirmed bug with gzip accept encoding header on Metrika's Logs API side. 
2. Data then being loaded into a **local ClickHouse instance**.  
   - If `ssh` in `ch_credentials.json` is null or false, data is stored locally.  
   - If `ssh` is set, an SSH connection is established, and data is transmitted to a remote ClickHouse instance.  
3. After all, data is either deleted or stored for future purposes, see more in global_config section. 
---

## Prerequisites  
  Before running the extractor, ensure you have:  

1. A [**Yandex Metrica Tag**](https://yandex.com/support/metrica/general/creating-counter.html).  
2. A Yandex account with at least **view access** to the Metrica tag/counter. [More details](https://yandex.com/support/metrica/general/access.html#guest).  
3. A **Yandex OAuth Token** with access to Yandex.Metrica. [Generate one here](https://yandex.com/dev/metrika/en/intro/authorization#get-oauth-token).  
4. A **ClickHouse instance** installed:  
   - Locally  
   - Remotely, but remote host accessible via SSH ( login+password authorization supported)
   - In a Docker container (either local or remote)  
   - If using a **remote ClickHouse instance**, SSH connection details must be provided in `ch_credentials.json` (currently supports only login+password authorization).  
5. A **database and table created in ClickHouse**:  
   - Currently, the script works with **one database, one table, and one Logs API entity at a time** (either sessions or events).  
   - To handle multiple sources, duplicate the script with different configurations.  
   - You can either name columns like entities of Logs API visits/hits table, or make more human-readable names. I.e.: either `ym:s:visitID` or `visitID`. In former case you will have more flexablity: for LogsAPI config you can leave all the fields to download with random order and it will be mapped to table columns by names. In latter case, you will need to order API fields in the same order as columns ordered in your table in database and always check if you have the same amount of fields in your logs api config file and in your ClickHouse table. 
   - Recommended table engine: `ReplacingMergeTree` (suited for Metrica Logs API data, as there are no `Sign` or `ver` fields).  
   - Recommended primary key (order by) for visits:  
     ```sql
     ORDER BY (visitID, counterUserIDHash, counterID)
     ```
     *(Omit `counterID` if using only one counter.)*  
  - Recommended primary key (order by) for hits: 
     ```sql
     ORDER BY (watchID, counterUserIDHash, counterID)
     ```
     *(Omit `counterID` if using only one counter.)*  
6. **Python 3.10+** and the corresponding `pip3`.  
7. Dependencies will be downloaded from requirements.txt file with pip: 
    ```bash
    pip install -r requirements.txt
    ``` 

---

## Config Files Description 

  This project relies on several JSON configuration files. All of them stored in `configs/` folder of the project.

#### `api_credentials.json`
  - `token` *(string)*: Your Yandex OAuth token (e.g., `"token": "d2_eerr534"`). [How to get one](https://yandex.com/dev/metrika/en/intro/authorization#get-oauth-token).  
  - `counter` *(string)*: Your Yandex counter ID (e.g., `"counter": "123456"`). 
  - `date1` *(string, nullable)*:  Start date to request data for. Leave it `null` to be yesterday. 
  - `date2` *(string, nullable)*: End date to request data for. Maximum value - the day before current one (yesterday). Leave in `null` to be yesterday. You can set date1 distinctly and date2 as `null` to request data since date1 till yesterday, or set both date1 and date2 nulls to request data only for yesterday. 
  - `fields` *(string)*: Comma-separated listing of fields (either for [sessions](https://yandex.com/dev/metrika/en/logs/fields/visits) or [events](https://yandex.com/dev/metrika/en/logs/fields/hits)).  
  - `source` *(string)*: Either `"visits"` for sessions or `"hits"` for events. 
  - `attribution` *(string)*: By default `"last"`, [see full values list](https://yandex.com/dev/metrika/en/logs/param).

  Example of filled `api_credentials.json` file: 
  
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

#### `ch_credentials.json`
  - `login` *(string)*: ClickHouse login.  
  - `password` *(string)*: ClickHouse password.  
  - `host` *(string)*: ClickHouse host address.  
  - `port` *(integer)*: ClickHouse port. Or local port to be bonded with remote ssh port. Can be null to automatically choose local port to bond.   
  - `db` *(string)*: ClickHouse database name.  
  - `table` *(string)*: ClickHouse table name.  
  - `logTable` *(string)*: ClickHouse table name for log of the script run. 
  - `ssh` *(null or dictionary)*: If null, data is loaded locally. If provided, SSH credentials must be specified. 

  Exmaple of `ch_credentials.json` file without ssh connection: 
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
  But if you need SSH connection, instead null set SSH connection properties: 
    - `login`: SSH username.  
    - `password`: SSH password.  
    - `host`: Remote machine hostname or IP.  
    - `port`: Local machine port bound to the remote port.  
    - `remote_port_bind`: Remote ClickHouse HTTP interface port (default **8123**). [More details](https://clickhouse.com/docs/en/interfaces/http#http-interface). Currently only HTTP supported. 

  Example: 
    ```json
    "ssh": {
      "login": "your_ssh_username",
      "password": "your_ssh_password",
      "host": "remote_host_address",
      "port": 22,
      "remote_port_bind": 8123
    }
    ``` 

#### `global_config.json`

  Contains next settings: 

	  - `log_continuous_path`: path to script's log. Will store history of all script runs. Default value: "logs/logs.log".logs folder may be abscent by default. You can create it manually or it will be created by the script automatically. 
    - `log_last_run_path`: path to script's last run log. Will store only info about last run. By default: "logs/last_run.log". 
    - `temporary_data_path`: path to store downloaded CSV files from Logs API. By default: "data/" folder. Each file will be named by formula: 
    `datetime-counterId-source(hits/visits)-part{part_number}.tsv`. Example: 2025-06-10 11:24:25-12345678-visits-part9.tsv 
    - `delete_temp_data`: boolean parameter. If true - deletes downloaded data, if false - stores it for some future purposes. Default: true.  
    - `delete_not_uploaded_to_db_temp_data`: boolean parameter. If true and `delete_temp_data` true, excludes those files, which weren't successfully downloaded to ClickHouse. By default: true. 
    - `api_strict_db_table_cols_names`: boolean parameter. If true, makes mapping matching between column names and downloaded column names of tsv files, only matched columns will be written to databse. Allows to ignore order of logs API fields set. If false, script will try to write all downloaded Logs API fields to ClickHouse in the same order. So it's very important to maintain the same order of Logs API reuired fields as columns order in database. By default: false. 
    - `run_db_table_test`: boolean parameter. If true, runs shallow test: checks whether database and table exists. If `continue_on_columns_test_fail` parameter (see the next one) is false and amount of columns in table and `api_credentials.json` file doesn't match - script will throw Error and stop. If `continue_on_columns_test_fail` is true, then execution of the script will continue. It makes sense only if you have the same names of columns in database as fields of Logs API and `api_strict_db_table_cols_names` parameter true, because in other case data won't be written to database and in case it's not deleted, you will need to upload it manually or to change configs and repeat the script execution. If database or table wasn't found - script will throw an error and stop working. In case this parameter is false, test will be skipped. Default: true.
    - `continue_on_columns_test_fail`: boolean parameter. Makes sense only if `run_db_table_test` is true, otherwise it's not significant. If true, will continue script execution in case columns amount in database is different from parameters amount in `api_credentials.json` file. It makes sense only if the names of columns in ClickHouse table are the same as Logs API params plus there is parameter api_strict_db_table_cols_names set true - in this case columns will be matched with parameters by names. Otherwise, data won't be written to database. Default: false.
    - `run_log_table_test`: boolean parameter. If true, checks if log table with name, set in `ch_credentials.json` `logTable` param exists and has expected columns. Skips check if false. Default: true. 
    - `create_log_table_on_fail`: boolean parameter. If true, creates logTable with name, set as `logTable` parameter in `ch_credentials.json` config file. If false - skips that, but log won't be written to logTable. Default: true. 
    - `continue_on_log_table_creation_fail`: boolean parameter. If true, continues script execution even if logTable doesn't exist/exists with wrong columns. Means, log won't be written to database. Default: false. 
    - `clear_api_queue`: boolean parameter. If true, clears Logs API queue of both prepared and pending logs to try to free enough space to prepare new Log request. If false: doesn't try to clear Logs API queue, means the queue will be preserved, but new Log reuqest highly likely doesn't have any chances to be created. Default value: true. 
    - `clear_created_logs_request`: boolean parameter. If true, clears log request that was created during current script run. It's a golden rule: don't leave a mess after yourself. Default: true. 
    - `frequency_api_status_check_sec`: integer parameter. Sets how often (in seconds) script will check if request was prepared to downloading or still in pedings (or returned some error). By default - 30 (seconds). 
    - `api_status_wait_timeout_min`: integer parameter. Defines, for how long script will wait till Logs API request will be prepared. By default - 30 (minutes).  
    - `data_loss_tolerance_perc`: integer parameter. Defines tolerancy level to dataloss (data that wasn't downloaded from Metrika server) in percents. By default - 10 (percent). 
    - `bad_data_tolerance_perc`: integer parameter. Defines tolerancy level for data not to be written to database. Means, how many percents of data can be not written to db without raising an error. By default - 15 (percent). 
    - `absolute_db_format_errors_tolerance`: integer parameter. Defines, how many errors in absolutes can be during uploading one file of data to databse withour raising an error. Default value: 10

  Example: 
    ```json
      {	
        "log_continuous_path": "logs/logs.log",
        "log_last_run_path": "logs/last_run.log", 
        "temporary_data_path": "data/",
        "delete_temp_data": true, 
        "delete_not_uploaded_to_db_temp_data": true, 
        "api_strict_db_table_cols_names": true, 
        "run_db_table_test":true, 
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
