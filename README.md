# Alaskan Weather Data _(alaska-etl)_

### "How accurate is the National Weather Service?"

#### By [Alejandro Socarras](https://alexsocarras.com)


ETL webcraping pipeline to compare NWS forecasts against data gathered by the US Climate Research Council (USCRN) throughout Alaska.

_This repo contains and builds off my work from a [team project](https://github.com/apsocarras/team-week3) at [Epicodus](https://www.epicodus.com/)._

![dashboard](img/alaska.gif)

_[Dashboard Presentation](https://lookerstudio.google.com/u/0/reporting/3d8306ba-0df6-42cb-bb90-f23924a0d2c6/)_

### _**Technologies Used**_ 
* Airflow 
* Google Cloud Platform (BigQuery, Looker Studio)
* Python (Pandas, Beautiful Soup)
* SQL

## Project Structure 
```bash
├── notebooks
│   ├── uscrn_scrape.ipynb 
│   └── uscrn_scrape.py     
├── airflow
│   ├── airflow.sh            # see install instructions 
│   ├── docker-compose.yaml   # see install instructions 
│   ├── dags
│   │   ├── nws_dag.py        # scrapes/uploads updates from NWS     
│   │   ├── uscrn_updates.py  # same for USCRN
│   │   └── utils
│   │       └── utils.py   
│   └── data
│       ├── nws_updates    
│       ├── uscrn_updates  
│       ├── sources.yaml      # URLs to data sources  
│       └── bq-config.yaml    # Name of your BQ project    
├── img
├── .gitignore
├── requirements.txt
└── README.md
```
`./notebooks/uscrn_scrape.ipynb` &nbsp;- &nbsp; Explains and contains code to scrape, transform, and upload the main USCRN data as well as supplemental data on column headers and descriptions.  

`./notebooks/uscrn_scrape.py` &nbsp; - &nbsp; Contains a python script to scrape all currently available data from the USCRN database. For a faster download, run this script separately to scrape the main dataset rather than the code in the notebook.

## Data Sources
[USCRN Hourly Historical Weather Data](https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/): This page contains hourly weather data from the U.S. Climate Reference Network / U.S. Regional Climate Reference Network (USCRN/USRCRN) stored in text files.

[NWS Forecasts](https://forecast.weather.gov/MapClick.php?lat=60.7506&lon=-160.5006&unit=0&lg=english&FcstType=digital): The National Weather Service has forecast offices in Fairbanks and Anchorage which provide hourly forecasts by coordinate location in AK. These are available in 48-Hour blocks up to four days out, stored in a tabular format. 
  

## Setup/Installation Requirements
```bash 
# Create and activate virtual environment
virtualenv -p python3.7 venv 
source venv/bin/activate

# Install packages from requirements.txt
pip install -r requirements.txt

# Install Airflow 
AIRFLOW_VERSION=2.3.2 
PYTHON_VERSION=3.7 
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Make subdirectories 
cd airflow 
mkdir ./logs/ ./plugins/
```

### **_Docker and BigQuery Setup_**

These instructions assume you have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed. 

```bash
# Download latest docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
# Set the .env variable
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
# (Optional) Download airflow.sh script for airflow CLI 
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.2/airflow.sh'

```
Prior to initializing Airflow in Docker, you will need to [create a project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) and an associated [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) in BigQuery. After downloading the account's credential file, you can configure your `docker-compose.yaml` to connect to BigQuery. 

```yaml 
  GOOGLE_APPLICATION_CREDENTIALS: /google_creds/<name-of-your-creds-file>.json
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
  - </path/to/your/creds/directory>:/google_creds
```
After opening Docker Desktop (or starting docker [via CLI](https://docs.docker.com/config/daemon/start/)): 

```bash
# Initialize and run airflow 
docker compose up airflow-init 
docker compose up 
```
Lastly, change `bq-config.yaml` to match your project-id.

```yaml
project-id: <your-project-id>
```
Be sure to have your Docker container up before running any of the DAGs. The files in `notebooks` do not require the container to be active, however.

## Known Bugs

* No known bugs


## License

MIT License

Copyright (c) 2023 Alejandro Socarras

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

</br>
