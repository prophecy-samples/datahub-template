# datahub-template

This project shows how easy it is to integrate the datahub schema registry for use in spark and prophecy. It features two main functions:

1. **browse datasets** - Browse and select existing datasets listed in datahub, synchronizing schema and other metadata. 
2. **sync dataset** - If you want to save dataset details to datahub, simply click "Sync To Datahub" to sync schema, descriptions and other metadata to datahub.

## Requirements 
1. A deployed instance of datahub, [**See here**](https://datahubproject.io/docs/category/deployment) for a guide to deploy datahub.
2. A datahub bearer token to authenticate requests, [**See here**](https://datahubproject.io/docs/api/graphql/token-management) for a guide to generate authentication tokens.

## Getting started

### 1. Specify datahub url and token:
Edit either in prophecy or in github: [project/gems/prophecysamples_datahubtemplate/gems/DatahubTable.py](https://github.com/prophecy-samples/datahub-template/blob/main/project/gems/prophecysamples_datahubtemplate/gems/DatahubTable.py#L49)

Set these to appropriate values:
```
    DATAHUB_BASE_URL = "<INSERT_DATAHUB_URL>"
    DATAHUB_TOKEN = "<INSERT_DATAHUB_TOKEN>"
```

### 2. Create a DatahubTable source/target.
![create-small](https://github.com/prophecy-samples/datahub-template/assets/2001660/c3bf9399-dd6f-4680-959d-5423fdb2669e)


### 3. Browse existing tables saved to Datahub.

![select-datahub](https://github.com/prophecy-samples/datahub-template/assets/2001660/fcc8ca22-c2c5-485b-93f5-b600a0cf5bf1)

### 4. Sync changes when there's an update.

![sync_to_datahub](https://github.com/prophecy-samples/datahub-template/assets/2001660/7e107d95-7b8b-4de6-b959-edd1e795a622)
