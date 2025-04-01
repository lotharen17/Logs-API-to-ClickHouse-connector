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
   - Remotely  
   - In a Docker container (either local or remote)  
   - If using a **remote ClickHouse instance**, SSH connection details must be provided in `ch_credentials.json`.  
5. A **database and table created in ClickHouse**:  
   - Currently, the script works with **one database, one table, and one Logs API entity at a time** (either sessions or events).  
   - To handle multiple sources, duplicate the script with different configurations.  
   - Recommended table engine: `ReplacingMergeTree` (suited for Metrica Logs API data, as there are no `Sign` or `ver` fields).  
   - Recommended primary key for visits:  
     ```sql
     ORDER BY (visitID, counterUserIDHash, counterID)
     ```
     *(Omit `counterID` if using only one counter.)*  
  
6. **Python 3.10+** and the corresponding `pip3`.  
7. Dependencies will be downloaded from requirements.txt file with pip: 
    ```bash
    pip install -r requirements.txt
    ``` 

---

## Config Files Description 

This project relies on several JSON configuration files:  

#### `api_credentials.json`
  - `token` *(string)*: Your Yandex OAuth token (e.g., `"token": "d2_eerr534"`). [How to get one](https://yandex.com/dev/metrika/en/intro/authorization#get-oauth-token).  
  - `counter` *(string)*: Your Yandex counter ID (e.g., `"counter": "123456"`). 
  - `date1` *(string, nullable)*:  Start date to request data for. Leave it `null` to be yesterday. 
  - `date2` *(string, nullable)*: End date to request data for. Maximum value - the day before current one (yesterday). Leave in `null` to be yesterday. You can set date1 distinctly and date2 as `null` to request data since date1 till yesterday, or set both date1 and date2 nulls to request data only for yesterday. 
  - `fields` *(string)*: Comma-separated listing of fields (either for [sessions](https://yandex.com/dev/metrika/en/logs/fields/visits) or [events](https://yandex.com/dev/metrika/en/logs/fields/hits)).  
  - `source` *(string)*: Either `"visits"` for sessions or `"hits"` for events. 
  - `attribution` *(string)*: By default `"last"`, [see full values list](https://yandex.com/dev/metrika/en/logs/param).

#### ch_credentials.json
  - `login` *(string)*: ClickHouse login.  
  - `password` *(string)*: ClickHouse password.  
  - `host` *(string)*: ClickHouse host address.  
  - `port` *(integer)*: ClickHouse port. Or local port to be bonded with remote ssh port. Can be null to automatically choose local port to bond.   
  - `db` *(string)*: ClickHouse database name.  
  - `table` *(string)*: ClickHouse table name.  
  - `logTable` *(string)*: ClickHouse table name for log of the script run. 
  - `ssh` *(null or dictionary)*: If null, data is loaded locally. If provided, SSH credentials must be specified:  

    ```json
    "ssh": {
      "login": "your_ssh_username",
      "password": "your_ssh_password",
      "host": "remote_host_address",
      "port": 22,
      "remote_port_bind": 8123
    }
    ```  

    - `login`: SSH username.  
    - `password`: SSH password.  
    - `host`: Remote machine hostname or IP.  
    - `port`: Local machine port bound to the remote port.  
    - `remote_port_bind`: Remote ClickHouse HTTP interface port (default **8123**). [More details](https://clickhouse.com/docs/en/interfaces/http#http-interface).  
