from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import boto3
from botocore.exceptions import ClientError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json

app = FastAPI()

origins = [
    "http://127.0.0.1",
    "http://127.0.0.1:8000",
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app = FastAPI()
client = boto3.client('glue',region_name='us-west-1')

app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/js", StaticFiles(directory="js"), name="scripts")

templates = Jinja2Templates(directory="templates")

#Read names
@app.get("/home", response_class=HTMLResponse)     
async def read_databases(request: Request):
    responseGetDatabases = client.get_databases()
    databaseList = responseGetDatabases['DatabaseList']
    databases = []
    for databaseDict in databaseList:
        databases.append(databaseDict['Name'])

    return templates.TemplateResponse("home.html", {"request": request, 'databases': databases})

#Read tables 
@app.get("/{database}", response_class=HTMLResponse)     
async def read_tables(request: Request, database:str):
    tablesData = []
    classifications = []
    tableInfo = []  
    responseGetTables = client.get_tables( DatabaseName = database )
    tableList = responseGetTables['TableList']
    for tableDict in tableList:
        tableName = tableDict['Name']

        # if "Parameters" not in tableDict:
        #     tableClassification = "-"
        # else:
        #     tableParameters = tableDict['Parameters']
        #     if "classification" not in tableParameters:
        #         tableClassification = "-"
        #     else:
        #         tableClassification = tableParameters["classification"]
        if "Parameters" in tableDict:
            tableParameters = tableDict['Parameters']
            print(tableParameters)
            if 'table_type' in tableParameters:
                tableClassification = tableParameters['table_type']
            elif "classification" in tableParameters:
                tableClassification = tableParameters["classification"]
            elif 'spark.sql.sources.provider' in tableParameters:
                tableClassification = tableParameters['spark.sql.sources.provider']
            else:
                tableClassification = tableDict['TableType']
        else:
            tableClassification = tableDict['TableType']

        tablesData.append(tableName)
        classifications.append(tableClassification)
        data = {}
        for d in tablesData:
            data['Table Name'] = d
        for v in classifications:
            data['Classification'] = v

        tableInfo.append(data)
    # print(tableList)
    return JSONResponse(tableInfo)

# Read columns
@app.get("/getColumns/{database}/{table}", response_class=HTMLResponse)  
async def read_columns(request: Request, database:str, table:str):
    colData = []
    colsInfo = []  
    responseGetTables = client.get_tables( DatabaseName = database )
    tableList = responseGetTables['TableList']
    # print(tableList)
    for tableDict in tableList:
        tableName = tableDict['Name']
        if tableName == table:
            columnsDict = tableDict['StorageDescriptor']
            columns = columnsDict['Columns']
            colData.append(columns)

    return JSONResponse(columns)