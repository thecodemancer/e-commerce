
# E-Commerce - Batch

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

```
gcloud config set project ${proyecto}
```

# Configurar la facturación

Verificar que la cuenta de facturación está asociada al proyecto. Ver [esta página](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#gcloud?hl=en).
```
gcloud beta billing projects describe ${proyecto}
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

```
gsutil mb gs://${bucket}
```

# Crear el dataset y la tabla en BigQuery

```
bq --location=${region} mk ${proyecto}:${dataset}
```
```
bq mk --schema='order_id:STRING,amount:FLOAT,profit:FLOAT,quantity:INTEGER,category:STRING,sub_category:STRING,order_date:DATE,customer_name:STRING,state:STRING,city:STRING,order_period:STRING,month_of_order_date:STRING,target:FLOAT,sales_target_period:STRING' --table ${proyecto}:${dataset}.${tabla}
```

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
      "name": "compute_project_id",
      "label": "Proyecto GCP para cómputo",
      "helpText": "ID del proyecto GCP donde se ejecutará el job."
    },
    {
      "name": "input_gcs",
      "label": "Input Bucket.",
      "helpText": " Nombre del bucket de donde se obrtendrán los datasets."
    },    
    {
      "name": "output_dataset",
      "label": "Dataset destino en BigQuery.",
      "helpText": "Nombre del dataset destino en BigQuery."
    },
    {
      "name": "output_table",
      "label": "Tabla destino en BigQuery.",
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
    --py-path "src/" \
    --py-path "src/processors/" \
    --env "FLEX_TEMPLATE_PYTHON_PY_FILE=e_commerce_batch.py" \
    --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt" \
    --env "FLEX_TEMPLATE_PYTHON_SETUP_FILE=setup.py"
```

# Ejecutar la Flex Template pipeline

```
gcloud dataflow flex-template run "e-commerce-batch-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://${bucket}/e_commerce_batch.json" \
    --region "${region}" \
    --network ${network_name} \
    --subnetwork ${subnetwork_url} \
    --parameters input_gcs="${bucket}" \
    --parameters output_table="${tabla}" \
    --parameters compute_project_id="${proyecto}" \
    --parameters output_dataset="${dataset}"     
```

# Revisar los resultados en BigQuery

```
bq query --use_legacy_sql=false 'SELECT * FROM `'"${proyecto}:${dataset}.${tabla}"'`'
```
