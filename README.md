# Cloud Spanner Connector
A custom Camunda outbound connector to perform database operations in Cloud Spanner Database.

## Supported Operations
|Operation           |                Description           |
|--------------------|--------------------------------------|
|Create Table | This operation creates a new table in the given database.|
|Insert Data | This operation will insert data in the given table of the specified database. |
|Update Data  | This operation is to update the data in a table based on where condition|
|Delete Data | This operation is used to delete a data in a table based on where condition|
|Retrieve Data | This operation will retrieve data from the spanner database. It also supports features like Limit, Sort and Filters.|


## Prereqisites
- Access credentials for your Google Cloud Storage project can be obtained through a Service Account. 
- To create the necessary Service Account, refer to the [Developer's Guide for creating a GCS service account](https://developers.google.com/workspace/guides/create-credentials#service-account)
- The IAM Service Account requires a role with appropriate permissions for object operations within a bucket. You can find more information about [cloud storage roles](https://cloud.google.com/storage/docs/access-control/iam-roles) and [cloud storage permissions](https://cloud.google.com/storage/docs/access-control/iam-permissions) 
- Follow the [Developers guide for creating credentials for a service account](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) to create and download a JSON key file.

- Make sure to define the complete path of the JSON key file as an environment variable  to the connector application

- The JSON file is formatted as follows
```json
{
    "type": "service_account",
    "project_id": "",
    "private_key_id": "",
    "private_key": "-----BEGIN PRIVATE KEY-----\n ...\n-----END PRIVATE KEY-----\n",
    "client_email": "",
    "client_id": "",
    "auth_uri": "",
    "token_uri": "",
    "auth_provider_x509_cert_url": "",
    "client_x509_cert_url": "",
    "universe_domain": "googleapis.com"
  }
  
```
## Connector Runtime 

Build a jar file with dependencies

```bash
mvn clean package
```
Build an image by using the Dockerfile 

```Dockerfile
FROM camunda/connectors:0.21.3

COPY target/spanner-connector-jar-with-dependencies.jar /opt/app/

ENTRYPOINT ["/start.sh"]
```
Command for BUilding an Image 

```bash
docker build -t spannerconnector:latest .
```

## Docker Compose  

- In this scenario, We are utilizing the self-managed version of Camunda 8, and we have created a corresponding Docker Compose file.

- A Docker volume is utilized to establish a connection between local storage and the container. 

- Ensure that the local storage you're mounting includes the necessary JSON credential file.

- You can utilize the `docker-compose.yaml` file to start a local connector runtime.


## Element Template

The element templates can be found in
the [spanner-connector.json](element-templates/spanner-connector.json) file.

## Spanner Connection

Spanner Connection details must be filled for all the operations to establish a connection between the connector and the spanner database of the user.

<img src="assets/spanner_connection.PNG" alt="spanner_connection.PNG">

## Spanner operations

These are the operations provided to do in Spanner database

<img src="assets/spanner_operations.png" alt="spanner_operations.png">



### Create Table 

<img src="assets/spanner_create.PNG" alt="spanner_create.PNG">

Columns List SampleJSON

```json
[
  {
    "colName": "bookId",
    "dataType": "INT64"
  },
   {
    "colName": "bookName",
    "dataType": "STRING(100)"
  },
   {
    "colName": "author",
    "dataType": "STRING(100)"
  }
  
]
```

### Insert Data 

<img src="assets/spanner_insert.PNG" alt="spanner_insert.PNG">

Data to be inserted SampleJSON

```json
[
  {
    "bookId": 1,
    "bookName": "\"Two States\"",
    "author": "\"Chetan Bhagat\""
  }
]
```

*Note: you can insert upto two data in a single instance.

```json
[
  {
    "bookId": 1,
    "bookName": "\"Two States\"",
    "author": "\"Chetan Bhagat\""
  },
  {
    "bookId": 2,
    "bookName": "\"Harry Potter\"",
    "author": "\"J K Rowling\""
  }
]
```

### Update data

<img src="assets/spanner_update.PNG" alt="spanner_update.PNG">


### Delete data

<img src="assets/spanner_delete.PNG" alt="spanner_delete.PNG">

### Retrieve data

<img src="assets/spanner_retrieve.PNG" alt="spanner_retrieve.PNG">

#### Note

- Columns to display: If "All" is selected, all the columns will be retrieved. If "Selected Columns" is selected, then only the selected columns will be retrieved

## Contact Information
For any queries and further support, please drop us a mail at camunda.support@acheron-tech.com
