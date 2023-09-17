# e-commerce
e-commerce

<img src="https://github.com/thecodemancer/e-commerce/blob/63dfe15f147e5eb22ae896aa3d861d78f007a2ef/img/e_commerce.jpeg" />

# Configuración

# Crear Proyecto

```
gcloud projects create formal-shell-295407
```

Seleccionar el proyecto

```gcloud config set project formal-shell-295407```

# Configurar la facturación

gcloud services enable dataflow compute_component logging storage_component storage_api bigquery pubsub cloudresourcemanager.googleapis.com appengine.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com cloudbuild.googleapis.com

# Configurar los permisos
```
gcloud projects add-iam-policy-binding formal-shell-295407 --member="user:davidregalado255@gmail.com" --role=roles/iam.serviceAccountUser
```

```
gcloud projects add-iam-policy-binding formal-shell-295407 --member="serviceAccount:999513749112-compute@developer.gserviceaccount.com" --role=roles/artifactregistry.reader
```

# Crear credenciales de autenticación local para la cuenta de Google:

```
gcloud auth application-default login
```

# Crear un Bucket en Google Cloud Storage

```gsutil mb gs://thecodemancer_us-east1```

# Create a Pub/Sub topic and a subscription to that topic

```gcloud pubsub topics create test1```
```gcloud pubsub subscriptions create --topic test1 test_suscription```

# Create a BigQuery dataset and table

```bq --location=us-east1 mk formal-shell-295407:us_east1_test_dataset```

```bq mk --table formal-shell-295407:us_east1_test_dataset.e_commerce url:STRING,review:STRING,last_date:TIMESTAMP,score:FLOAT,first_date:TIMESTAMP,num_reviews:INTEGER```

# Setup del ambiente de desarrollo

# Crear Artifact Registry

```gcloud artifacts repositories create test-artifact-repository --repository-format=docker --location=us-east1 --async```

Antes de poder enviar o extraer imágenes, configure Docker para autenticar solicitudes de Artifact Registry. Para configurar la autenticación en los repositorios de Docker, ejecute el siguiente comando:

```gcloud auth configure-docker us-east1-docker.pkg.dev```

# Metadata

Crear un archivo ```metadata.json``` y guardarlo en Google Cloud Storage

```
{
  "name": "Streaming beam Python flex template",
  "description": "Streaming beam example for python flex template.",
  "parameters": [
    {
      "name": "input_subscription",
      "label": "Input PubSub subscription.",
      "helpText": "Name of the input PubSub subscription to consume from.",
      "regexes": [
        "projects/[^/]+/subscriptions/[a-zA-Z][-_.~+%a-zA-Z0-9]{2,}"
      ]
    },
    {
      "name": "output_table",
      "label": "BigQuery output table name.",
      "helpText": "Name of the BigQuery output table name.",
      "isOptional": true,
      "regexes": [
        "([^:]+:)?[^.]+[.].+"
      ]
    }
  ]
}
```

# Crear la Flex Template

```
gcloud dataflow flex-template build gs://thecodemancer_us-east1/samples/dataflow/templates/streaming-beam-sql.json \
     --image-gcr-path "us-east1-docker.pkg.dev/formal-shell-295407/test-artifact-repository/dataflow/streaming-beam-sql:latest" \
     --sdk-language "PYTHON" \
     --flex-template-base-image "PYTHON3" \
     --metadata-file "metadata.json" \
     --py-path "." \
     --env "FLEX_TEMPLATE_PYTHON_PY_FILE=streaming_beam.py" \
     --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"
```

# Ejecutar la Flex Template pipeline

```
gcloud dataflow flex-template run "streaming-beam-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://thecodemancer_us-east1/samples/dataflow/templates/streaming-beam-sql.json" \
    --parameters input_subscription="projects/formal-shell-295407/subscriptions/test_suscription" \
    --parameters output_table="formal-shell-295407:us_east1_test_dataset.e_commerce" \
    --region "us-east1"
```

# Revisar los resultados en BigQuery

```
bq query --use_legacy_sql=false 'SELECT * FROM `'"formal-shell-295407.us_east1_test_dataset.e_commerce"'`'
```
