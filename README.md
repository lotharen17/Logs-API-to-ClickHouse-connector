# Extractor + Connector for Yandex.Metrica Logs API to Local ClickHouse (OOP Version)  

---

## Quick Start  

Ensure you have **Python 3.10+**, **ClickHouse** installed, and a database with at least one table created.  
Then:  
1. Fill in the configuration files (`.json` files). [See more details below](#config-files).  
2. Run `main.py`.  
3. Enjoy your Logs API data in ClickHouse!  

---

## Description  

This extractor was developed as a pet project to pull data from the [**Yandex Metrica Logs API**](https://yandex.com/dev/Metrica/en/logs/) and store it in a **local** or **remote** [**ClickHouse**](https://clickhouse.com/) instance.  

It works seamlessly with both:  
- [**Sessions/Visits Table**](https://yandex.com/dev/Metrica/en/logs/fields/visits)  
- [**Events/Hits Table**](https://yandex.com/dev/Metrica/en/logs/fields/hits)  

Configuration is flexible via multiple JSON files. [See the config files section](#config-files).  

### Automation  
Extractor execution can be automated using:  
- **Cron Jobs**  
- **Apache Airflow**  
- **Druid**  
- **Any Other Orchestration Tool**  

### How It Works  
1. Extractor downloads data in **gziped TSV format** to the local machine (the same folder as this script).  
2. Data is then loaded into a **local ClickHouse instance**.  
   - If `ssh` in `ch_credentials.json` is null, data is stored locally.  
   - If `ssh` is set, an SSH connection is established, and data is transmitted to a remote ClickHouse instance.  

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
   - Recommended primary key:  
     ```sql
     ORDER BY (visitID, counterUserIDHash, counterID)
     ```
     *(Omit `counterID` if using only one counter.)*  
6. **Python 3.10+** and the corresponding `pip3`.  

---

## Config Files  

This project relies on several JSON configuration files:  

- **`source_fields.json`**  
  - `fields` *(string)*: Comma-separated list of fields (either for [sessions](https://yandex.com/dev/metrika/en/logs/fields/visits) or [events](https://yandex.com/dev/metrika/en/logs/fields/hits)).  
  - `source` *(string)*: Either `"visits"` for sessions or `"hits"` for events.  

- **`token_counter.json`**  
  - `token` *(string)*: Your Yandex OAuth token (e.g., `"token": "d2_eerr534"`). [How to get one](https://yandex.com/dev/metrika/en/intro/authorization#get-oauth-token).  
  - `counter` *(string)*: Your Yandex counter ID (e.g., `"counter": "123456"`).  

- **`ch_credentials.json`**  
  - `login` *(string)*: ClickHouse login.  
  - `password` *(string)*: ClickHouse password.  
  - `host` *(string)*: ClickHouse host address.  
  - `port` *(integer)*: ClickHouse port.  
  - `db` *(string)*: ClickHouse database name.  
  - `table` *(string)*: ClickHouse table name.  
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
