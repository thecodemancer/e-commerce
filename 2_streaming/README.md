
# E-Commerce - Streaming

<img src="https://github.com/thecodemancer/e-commerce/blob/63dfe15f147e5eb22ae896aa3d861d78f007a2ef/img/e_commerce.jpeg" />

# Configuración

Creamos variables que se usarán durante la configuración

```
source .env
```

# Crear Proyecto

```
gcloud projects create ${proyecto}
```

Seleccionar el proyecto

```gcloud config set project ${proyecto}```

# Configurar la facturación

Verificar que la cuenta de facturación está asociada al proyecto. Ver [esta página](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#gcloud?hl=en).
```
gcloud beta billing projects describe thecodemancer-e-commerce-12345
```

# Habilitar APIs

```
gcloud services enable dataflow compute_component logging storage_component storage_api bigquery pubsub cloudresourcemanager.googleapis.com artifactregistry.googleapis.com
```

# Crear credenciales de autenticación local para la cuenta de Google:

```
gcloud auth application-default login
```


# Configurar los permisos
```
gcloud projects add-iam-policy-binding ${proyecto} --member="user:${correo}" --role=roles/iam.serviceAccountUser
```

```
gcloud projects add-iam-policy-binding ${proyecto} --member="serviceAccount:${proyecto_numero}-compute@developer.gserviceaccount.com" --role=roles/dataflow.admin
gcloud projects add-iam-policy-binding ${proyecto} --member="serviceAccount:${proyecto_numero}-compute@developer.gserviceaccount.com" --role=roles/dataflow.worker
gcloud projects add-iam-policy-binding ${proyecto} --member="serviceAccount:${proyecto_numero}-compute@developer.gserviceaccount.com" --role=roles/bigquery.dataEditor
gcloud projects add-iam-policy-binding ${proyecto} --member="serviceAccount:${proyecto_numero}-compute@developer.gserviceaccount.com" --role=roles/pubsub.editor
gcloud projects add-iam-policy-binding ${proyecto} --member="serviceAccount:${proyecto_numero}-compute@developer.gserviceaccount.com" --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding ${proyecto} --member="serviceAccount:${proyecto_numero}-compute@developer.gserviceaccount.com" --role=roles/artifactregistry.reader
```


# Crear un Bucket en Google Cloud Storage

```gsutil mb gs://${bucket}```

# Crear el dataset y la tabla en BigQuery

```bq --location=${region} mk ${proyecto}:${dataset}```

```bq mk --table ${proyecto}:${dataset}.${tabla} schema='month_of_order_date:STRING,category:STRING,target:FLOAT64,order_id:STRING,order_date:STRING,customer_name:STRING,state:STRING,city:STRING,amount:FLOAT64,profit:FLOAT64,quantity:INT64,category:STRING,sub_category:STRING',```

# Crear Artifact Registry

```gcloud artifacts repositories create ${artifact_registry_name} --repository-format=docker --location=${region} --async```

Antes de poder enviar o extraer imágenes, configure Docker para autenticar solicitudes de Artifact Registry. Para configurar la autenticación en los repositorios de Docker, ejecute el siguiente comando:

```gcloud auth configure-docker ${region}-docker.pkg.dev```

# Crear archivo de requirements

```echo "apache-beam[gcp]==2.41.0" > requirements.txt```

# Metadata

Crear un archivo ```metadata.json``` y guardarlo en Google Cloud Storage

```
{
  "name": "E-Commerce Python flex template",
  "description": "Procesamiento batch para el proyecto E-Commerce.",
  "parameters": [
    {
      "name": "input_gcs",
      "label": "Input Bucket",
      "helpText": "Nombre del bucket de donde se obtendrán los datasets."
    },
    {
      "name": "output_table",
      "label": "Tabla destino en BigQuery",
      "helpText": "Nombre de la tabla destino en BigQuery."
    }
  ]
}
```

# Crear la Flex Template

```
gcloud dataflow flex-template build gs://${bucket}/e_commerce_batch.json \
     --image-gcr-path "${region}-docker.pkg.dev/${proyecto}/${artifact_registry_name}/dataflow/e_commerce_batch:latest" \
     --sdk-language "PYTHON" \
     --flex-template-base-image "PYTHON3" \
     --metadata-file "metadata.json" \
     --py-path "." \
     --env "FLEX_TEMPLATE_PYTHON_PY_FILE=e_commerce_batch.py" \
     --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"
```

# Ejecutar la Flex Template pipeline

```
gcloud dataflow flex-template run "e_commerce_batch-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://thecodemancer_us-east1/samples/dataflow/templates/e_commerce_batch.json" \
    --parameters input_gcs="projects/formal-shell-295407/subscriptions/test_suscription" \
    --parameters output_table="formal-shell-295407:us_east1_test_dataset.e_commerce" \
    --region "us-east1"
```

# Revisar los resultados en BigQuery

```
bq query --use_legacy_sql=false 'SELECT * FROM `'"formal-shell-295407.us_east1_test_dataset.e_commerce"'`'
```
