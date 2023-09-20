from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
import requests, json, traceback


class DatahubTableFormat(DatasetSpec):
    name: str = "DatahubTable"
    datasetType: str = "Database"
    mode: str = "batch"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class DatahubTableProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        path: str = ""
        tableName: str = ""
        useExternalFilePath: Optional[bool] = False
        externalFilePath: Optional[str] = ""
        timestampAsOf: Optional[str] = None
        versionAsOf: Optional[str] = None
        writeMode: Optional[str] = "error"
        partitionColumns: Optional[List[str]] = None
        replaceWhere: Optional[str] = None
        insertInto: Optional[bool] = None
        overwriteSchema: Optional[bool] = None
        mergeSchema: Optional[bool] = None
        optimizeWrite: Optional[bool] = None
        fileFormat: Optional[str] = "parquet"
        provider: Optional[str] = "delta"
        filterQuery: Optional[str] = ""
        isCatalogEnabled: Optional[bool] = None
        catalog: Optional[str] = None
        datahubDataset: Optional[str] = None
        datahubUrn: Optional[str] = None
        datahubPlatform: str = "databricks"
        datahubEnv: str = "DEV"


    DATAHUB_BASE_URL = "<INSERT_DATAHUB_URL>"
    DATAHUB_TOKEN = "<INSERT_DATAHUB_TOKEN>"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DATAHUB_TOKEN}"
    }

    exception = None
    
    type_map = {
        "long": "NumberType",
        "string": "StringType",
        "boolean": "BooleanType",
        "double": "NumberType",
        "date": "DateType",
        "timestamp": "TimeType",
        "array": "ArrayType",
        "struct": "RecordType",
        "integer": "NumberType",
        "float": "NumberType",
        "byte": "BytesType",
        "short": "NumberType",
        "binary": "BytesType",
        "decimal": "NumberType",
        "null": "NullType"
    }

    def construct_schema(self, datahub_dataset):

        datahub_schema = datahub_dataset["schemaMetadata"]
        editableSchemaFieldInfo = (datahub_dataset.get("editableSchemaMetadata") or {}).get("editableSchemaFieldInfo") or []
        fieldsToDesc = {}
        for field in editableSchemaFieldInfo:
            fieldsToDesc[field["fieldPath"]] = field["description"]
        fields = []
        for f in datahub_schema["fields"]:
            desc = fieldsToDesc.get(f["fieldPath"], f.get("description"))
            if desc:
                field = {"name": f["fieldPath"], "type": f["nativeDataType"], "nullable": f["nullable"], "metadata": {"description": desc }}
            else:
                field = {"name": f["fieldPath"], "type": f["nativeDataType"], "nullable": f["nullable"], "metadata": {}}
            fields.append(field)
        schema_json = {"type": "struct", "fields": fields}
        schema = StructType.fromJson(schema_json)
        return schema


    def post_dataset_schema(self, schema, platform, dataset, env):
        url = f"{self.DATAHUB_BASE_URL}/openapi/entities/v1/"

        fields = []
        for field in schema.fields:
            field_dict = {
                "fieldPath": field.name,
                "jsonPath": field.name,
                "nullable": field.nullable,
                "description": field.metadata.get("description"),
                "type": {
                    "type": {
                    "__type": self.type_map[field.dataType.typeName()]
                    }
                },
                "nativeDataType": field.dataType.typeName(),
                "recursive": False
            }
            fields.append(field_dict)

        payload = json.dumps([
        {
            "entityType": "dataset",
            "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset},{env})",
            "aspect": {
            "__type": "SchemaMetadata",
            "schemaName": "CatalogTableSchema",
            "platform": f"urn:li:dataPlatform:{platform}",
            "platformSchema": {
                "__type": "PrestoDDL",
                "tableSchema": "tableSchema",
                "rawSchema": "rawSchema"
            },
            "version": 0,
            "hash": f"{platform}/{dataset}/{env}",
            "fields": fields
            }
        }
        ])
        

        response = requests.request("POST", url, headers=self.headers, data=payload)

        return response

    def update_description(self, urn: str, description: str):

        url = f"{self.DATAHUB_BASE_URL}/api/graphql"

        query = """
            mutation updateDesc($urn: String!, $desc: String!){
            updateDataset(urn: $urn, input:{
                editableProperties: {
                description: $desc
                }
            }){
                urn
                editableProperties{
                description
                }
            }
            }
        """
        variables = {
            "urn": urn,
            "desc": description
        }

        payload = json.dumps({
            "query": query,
            "variables": variables
        })

        response = requests.request("POST", url, headers=self.headers, data=payload)

        return response.json()


    def onButtonClick(self, state: Component[DatahubTableProperties]):
        try:
            props = state.properties
            table_name = f"{props.catalog}.{props.path}.{props.tableName}" if props.isCatalogEnabled else f"{props.path}.{props.tableName}"
            schema = state.properties.schema
            platform = "databricks"
            env = "DEV"
            datahubUrn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table_name},{env})"
            self.post_dataset_schema(schema, platform, table_name, env)
            self.update_description(datahubUrn, props.description)
            self.exception = None
            return state.bindProperties(replace(props, datahubUrn=datahubUrn))
        except Exception:
            self.exception = traceback.format_exc()
        return state

    def browse_datasets(self, path: list, start: int = 0, count: int = 100):

        url = f"{self.DATAHUB_BASE_URL}/api/graphql"

        query = """
        query browseDatasets($path: [String!], $start: Int, $count: Int) {
        browse(input: {type: DATASET, path: $path, start: $start, count: $count}) {
            total
            entities {
                urn
                type
                ... on Dataset{
                    name
                }
            }
            groups {
                name
                __typename
                count
            }
        }
        }
        """
        variables = {
            "path": path,
            "start": start,
            "count": count
        }

        payload = json.dumps({
            "query": query,
            "variables": variables
        })

        response = requests.request("POST", url, headers=self.headers, data=payload)

        return response.json()


    def get_dataset(self, urn: str):

        url = f"{self.DATAHUB_BASE_URL}/api/graphql"

        query = """
        query getDataset($urn: String!) {
            dataset(urn: $urn) {
                urn
                name
                schemaMetadata {
                    fields {
                        fieldPath
                        nullable
                        type
                        nativeDataType
                        description
                    }
                }
                editableProperties {
                    description
                }
                editableSchemaMetadata {
                    editableSchemaFieldInfo {
                        fieldPath
                        description
                    }
                }
            }
        }
        """
        variables = {
            "urn": urn,
        }

        payload = json.dumps({
            "query": query,
            "variables": variables
        })

        response = requests.request("POST", url, headers=self.headers, data=payload)

        return response.json()

    def get_all_datasets(self, path: list):
        start = 0
        count = 100
        total = 0
        entities = []
        while True:
            response = self.browse_datasets(path, start, count)
            if response["data"]["browse"]["groups"]:
                for group in response["data"]["browse"]["groups"]:
                    entities.extend(self.get_all_datasets(path + [group["name"]]))
            else: 
                entities.extend(response["data"]["browse"]["entities"])
            total = response["data"]["browse"]["total"]
            start += count
            if start >= total:
                break
        return entities

    def getDatahubSelect(self):
        datasets = self.get_all_datasets(["dev", "databricks"])
        options = [""]
        for dataset in datasets:
            options.append(dataset["name"])
        box = SelectBox("Datahub Dataset")
        for option in options:
            box = box.addOption(option, option)
        return box.bindProperty("datahubDataset")


    def sourceDialog(self) -> DatasetDialog:
        fieldPicker = FieldPicker(height=("100%")) \
            .addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True
        ).addField(
            SelectBox("Provider")
                .addOption("delta", "delta")
                .addOption("hive", "hive"),
            "provider",
            True
        )

        return DatasetDialog("DatahubTable") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(
                    CatalogTableDB("").bindProperty("path").bindTableProperty("tableName").bindCatalogProperty(
                        "catalog").bindIsCatalogEnabledProperty("isCatalogEnabled"))
                    .addElement(
                        self.getDatahubSelect()
                    )
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                StackLayout().addElement(
                        StackItem().addElement(
                            SimpleButtonLayout("Sync to Datahub", self.onButtonClick)
                        )
                    )
                    .addElement(
                    Condition()
                        .ifEqual(
                        PropExpr("component.properties.provider"),
                        StringExpr("delta"),
                    )
                        .then(
                        StackLayout(height=("100%")).addElement(
                            StackItem(grow=(1)).addElement(
                                fieldPicker
                                    .addField(TextBox("Read timestamp").bindPlaceholder(""), "timestampAsOf")
                                    .addField(TextBox("Read version").bindPlaceholder(""), "versionAsOf")
                            ).addElement(
                                ScrollBox()
                                    .addElement(TitleElement(title="Filter Predicate"))
                                    .addElement(Editor(height=("30bh")).bindProperty("filterQuery"))
                            )
                        )
                    ).otherwise(
                        StackLayout(height=("100%")).addElement(
                            StackItem(grow=(1)).addElement(
                                fieldPicker
                            ).addElement(
                                ScrollBox()
                                    .addElement(TitleElement(title="Filter Predicate"))
                                    .addElement(Editor(height=("40bh")).bindProperty("filterQuery"))
                            )
                        )
                    )
                ),
                "auto")
                .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:

        fieldPicker = FieldPicker(height=("100%")) \
            .addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True
        ).addField(
            SelectBox("Provider")
                .addOption("delta", "delta")
                .addOption("hive", "hive"),
            "provider",
            True
        )

        deltaFieldPicker = fieldPicker.addField(
            SelectBox("Write Mode")
                .addOption("overwrite", "overwrite")
                .addOption("error", "error")
                .addOption("append", "append")
                .addOption("ignore", "ignore")
                .addOption("merge", "merge")
                .addOption("scd2 merge", "merge_scd2"),
            "writeMode", True
        ).addField(Checkbox("Use insert into"), "insertInto") \
            .addField(Checkbox("Overwrite table schema"), "overwriteSchema") \
            .addField(Checkbox("Merge dataframe schema into table schema"), "mergeSchema") \
            .addField(
            SchemaColumnsDropdown("Partition Columns")
                .withMultipleSelection()
                .bindSchema("schema")
                .showErrorsFor("partitionColumns"),
            "partitionColumns"
        ) \
            .addField(TextBox("Overwrite partition predicate").bindPlaceholder(""),
                      "replaceWhere") \
            .addField(Checkbox("Optimize write"), "optimizeWrite")

        hiveFieldPicker = fieldPicker.addField(
            SelectBox("Write Mode")
                .addOption("overwrite", "overwrite")
                .addOption("error", "error")
                .addOption("append", "append")
                .addOption("ignore", "ignore"),
            "writeMode", True
        ).addField(
            SelectBox("File Format")
                .addOption("sequencefile", "sequencefile")
                .addOption("rcfile", "rcfile")
                .addOption("orc", "orc")
                .addOption("parquet", "parquet")
                .addOption("textfile", "textfile")
                .addOption("avro", "avro"),
            "fileFormat"
        ).addField(Checkbox("Use insert into"), "insertInto") \
            .addField(
            SchemaColumnsDropdown("Partition Columns")
                .withMultipleSelection()
                .bindSchema("schema")
                .showErrorsFor("partitionColumns"),
            "partitionColumns"
        )

        return DatasetDialog("DatahubTable") \
            .addSection(
            "LOCATION",
            StackLayout()
                .addElement(CatalogTableDB("").bindProperty("path").bindTableProperty("tableName").bindCatalogProperty(
                "catalog").bindIsCatalogEnabledProperty("isCatalogEnabled")).addElement(
                        self.getDatahubSelect()
                    )
                .addElement(Checkbox("Use File Path").bindProperty("useExternalFilePath"))
                .addElement(
                Condition()
                    .ifEqual(
                    PropExpr("component.properties.useExternalFilePath"),
                    BooleanExpr(True),
                )
                    .then(
                    TextBox(
                        "File location", placeholder="dbfs:/FileStore/delta/tableName"
                    ).bindProperty("externalFilePath")
                )
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                StackLayout().addElement(
                    StackLayout(height=("100%")).addElement(
                        StackItem().addElement(
                            SimpleButtonLayout("Sync to Datahub", self.onButtonClick)
                        )
                    ).addElement(
                        StackItem(grow=(1)).addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.provider"),
                                StringExpr("delta"),
                            ).then(deltaFieldPicker)
                                .otherwise(hiveFieldPicker)
                        )
                    )
                ),
                "auto"
            )
                .addColumn(
                SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(DatahubTableFormat, self).validate(context, component)
        import re
        NAME_PATTERN = re.compile(r"^[\w]+$")
        CONFIG_NAME_PATTERN = re.compile(r"^\$(.*)$")

        props = component.properties

        if component.properties.isCatalogEnabled and (len(component.properties.catalog) == 0):
            diagnostics.append(
                Diagnostic("properties.catalog", "Catalog Name cannot be empty [Location]", SeverityLevelEnum.Error))

        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "Database Name cannot be empty [Location]", SeverityLevelEnum.Error))

        if len(component.properties.tableName) == 0:
            diagnostics.append(
                Diagnostic("properties.tableName", "Table Name cannot be empty [Location]", SeverityLevelEnum.Error))

        if component.properties.useExternalFilePath and isBlank(component.properties.externalFilePath):
            diagnostics.append(
                Diagnostic(f"properties.useExternalFilePath", "File Location cannot be empty", SeverityLevelEnum.Error))

        if not isBlank(props.versionAsOf) and parseInt(props.versionAsOf) is None:
            diagnostics.append(
                Diagnostic("properties.versionAsOF", "Invalid version [Properties]", SeverityLevelEnum.Error))

        if props.provider == "hive" and props.writeMode in ["merge", "merge_scd2"]:
            diagnostics.append(
                Diagnostic("properties.writeMode", "Please select valid write mode from dropdown",
                           SeverityLevelEnum.Error))
        
        if self.exception is not None:
            diagnostics.append(
                Diagnostic("properties.datahubDataset", self.exception, SeverityLevelEnum.Error))

        return diagnostics


    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        # super(DatahubTableFormat, self).onChange(context, oldState, newState)
        oldProps = oldState.properties
        newProps = newState.properties
        
        try:        
            update_schema = False
            table_changed = newProps.tableName != oldProps.tableName or newProps.path != oldProps.path or newProps.catalog != oldProps.catalog or newProps.isCatalogEnabled != oldProps.isCatalogEnabled
            is_defined = newProps.tableName is not None and newProps.path is not None and newProps.catalog is not None and newProps.isCatalogEnabled is not None
            if newProps.datahubDataset != oldProps.datahubDataset:
                if newProps.datahubDataset:
                    catalogEnabled = len([c for c in newProps.datahubDataset if c =="."]) == 2
                    catalog = None
                    path = None
                    table_name = None
                    update_schema = True
                    if catalogEnabled:
                        catalog = newProps.datahubDataset.split(".")[0]
                        path = newProps.datahubDataset.split(".")[1]
                        tableName = newProps.datahubDataset.split(".")[2]
                    else:
                        path = newProps.datahubDataset.split(".")[0]
                        tableName = newProps.datahubDataset.split(".")[1]
                    datahubUrn = f"urn:li:dataset:(urn:li:dataPlatform:{newProps.datahubPlatform},{newProps.datahubDataset},{newProps.datahubEnv})"
                    dataset = self.get_dataset(datahubUrn)
                    schema = self.construct_schema(dataset["data"]["dataset"])
                    description = (dataset["data"]["dataset"]["editableProperties"] or {}).get("description") or ""
                    newState = newState.bindProperties(replace(
                        newProps,
                        datahubUrn=datahubUrn, 
                        isCatalogEnabled=catalogEnabled,
                        catalog=catalog,
                        path=path,
                        tableName=tableName,
                        schema=schema,
                        description=description
                    ))
            elif table_changed and is_defined:
                datahubDataset = f"{newProps.catalog}.{newProps.path}.{newProps.tableName}" if newProps.isCatalogEnabled else f"{newProps.path}.{newProps.tableName}"
                datahubUrn = f"urn:li:dataset:(urn:li:dataPlatform:{newProps.datahubPlatform},{datahubDataset},{newProps.datahubEnv})"
                newState = newState.bindProperties(replace(newProps, datahubUrn=datahubUrn, datahubDataset=datahubDataset))

            self.exception = None
        except Exception:
            self.exception = traceback.format_exc()
        return newState

    class DatahubTableFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: DatahubTableFormat.DatahubTableProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            table_name = f"{self.props.catalog}.{self.props.path}.{self.props.tableName}" if self.props.isCatalogEnabled else f"{self.props.path}.{self.props.tableName}"
            if self.props.provider == "delta":
                if not isBlank(self.props.filterQuery):
                    if self.props.versionAsOf is not None:
                        df = spark.sql(
                            f'SELECT * FROM {table_name} VERSION AS OF {self.props.versionAsOf} WHERE {self.props.filterQuery}')
                    elif self.props.timestampAsOf is not None:
                        df = spark.sql(
                            f'SELECT * FROM {table_name} TIMESTAMP AS OF "{self.props.timestampAsOf}" WHERE {self.props.filterQuery}')
                    else:
                        df = spark.sql(
                            f'SELECT * FROM {table_name} WHERE {self.props.filterQuery}')
                elif self.props.versionAsOf is not None:
                    df = spark.sql(
                        f'SELECT * FROM {table_name} VERSION AS OF {self.props.versionAsOf}')
                elif self.props.timestampAsOf is not None:
                    df = spark.sql(
                        f'SELECT * FROM {table_name} TIMESTAMP AS OF "{self.props.timestampAsOf}"')
                else:
                    df = spark.read.table(table_name)
            elif self.props.provider == "hive":
                if not isBlank(self.props.filterQuery):
                    df = spark.sql(
                        f'SELECT * FROM {table_name} WHERE {self.props.filterQuery}')
                else:
                    df = spark.read.table(table_name)
            else:
                df = spark.read.table(table_name)
            return df

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            table_name = f"{self.props.catalog}.{self.props.path}.{self.props.tableName}" if self.props.isCatalogEnabled else f"{self.props.path}.{self.props.tableName}"
            
            writer = in0.write.format(self.props.provider)
            if self.props.provider == "delta" and self.props.optimizeWrite is not None:
                writer = writer.option("optimizeWrite", self.props.optimizeWrite)
            if self.props.provider == "hive":
                writer = writer.option("fileFormat", self.props.fileFormat)
            if self.props.provider == "delta" and self.props.mergeSchema is not None:
                writer = writer.option("mergeSchema", self.props.mergeSchema)
            if self.props.provider == "delta" and self.props.replaceWhere is not None:
                writer = writer.option("replaceWhere", self.props.replaceWhere)
            if self.props.provider == "delta" and self.props.overwriteSchema is not None:
                writer = writer.option("overwriteSchema", self.props.overwriteSchema)
            if self.props.useExternalFilePath:
                writer = writer.option("path", self.props.externalFilePath)

            if self.props.writeMode is not None:
                if self.props.writeMode in ["merge", "merge_scd2"]:
                    writer = writer.mode("overwrite")
                else:
                    writer = writer.mode(self.props.writeMode)

            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)

            if self.props.insertInto:
                writer.insertInto(table_name)
            else:
                writer.saveAsTable(table_name)
