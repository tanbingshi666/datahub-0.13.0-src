# DataHub Metadata Ingestion Docker Image
[![datahub-ingestion docker](https://github.com/datahub-project/datahub/actions/workflows/docker-ingestion.yml/badge.svg)](https://github.com/datahub-project/datahub/actions/workflows/docker-ingestion.yml)

Refer to the [metadata ingestion framework](../../metadata-ingestion) to understand the architecture and responsibilities of this service.

## Slim vs Full Image Build

There are two versions of this image. One includes pyspark and Oracle dependencies and is larger due to the java dependencies.

Running the standard build results in the `slim` image without pyspark being generated by default. In order to build the full
image with pyspark use the following project property `-PdockerTarget=full`.