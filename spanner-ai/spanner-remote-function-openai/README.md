# How to Generate OpenAI Embeddings with Spanner Remote UDFs

You can use the CREATE MODEL keyword in Spanner to set up a GCP VertexAI endpoint to generate embeddings and leverage other AI features.

This guide outlines how to use Spanner's Remote UDF capability to call *3rd party models* (OpenAI, in this instance) for generating text embeddings. We will create a Python function hosted on Cloud Run that adheres to the Spanner Remote UDF protocol, secure the API key using Secret Manager, and invoke the model directly from a SQL query.

NOTE: While this guide focuses on generating embeddings, you can use this remote UDF based approach to integrate with 3rd party models for other purposes as well.

---

## Prerequisites

1. **Google Cloud Project** with the following APIs enabled:
* Spanner API
* Cloud Run API
* Secret Manager API
* Cloud Build API (required for Cloud Run Functions)


2. **OpenAI Account & API Key**:
* Sign up at [platform.openai.com](https://platform.openai.com/).
* Generate a new key under **Dashboard > API keys**.


3. **gcloud CLI** installed and authenticated.

---

## Step 1: Securely Store the OpenAI API Key

We will use Google Cloud Secret Manager to store the OpenAI API key securely, avoiding hardcoded secrets in our code.

1. **Create the secret:**
```bash
gcloud secrets create openai-api-key --replication-policy="automatic"

```


2. **Add your API key version:**
(Replace `YOUR_OPEN_AI_KEY` with your actual key starting with `sk-...`)
```bash
echo -n "YOUR_OPEN_AI_KEY" | gcloud secrets versions add openai-api-key --data-file=-

```



---

## Step 2: Create the Python Cloud Run Function

We will write a Python function that uses the `Flask` framework to implement the Remote UDF protocol. It accepts a batch of text inputs, calls the OpenAI API, and returns the embeddings.

1. **Create a directory** and navigate into it:
```bash
mkdir spanner-openai-func
cd spanner-openai-func

```


2. **Create `requirements.txt`**:
```text
functions-framework==3.*
flask
openai
google-cloud-secret-manager

```

3. **Create [`main.py`](src/main.py)**:
This script implements the required protocol: handling the `{"calls": [...]}` payload, batching the request to OpenAI, and returning `{"replies": [...]}` using `flask.jsonify`.
```python
import functions_framework
from flask import jsonify
from google.cloud import secretmanager
from openai import OpenAI
import urllib.request

# Global client to reuse across warm instances
openai_client = None

def get_openai_client():
    global openai_client
    if openai_client:
        return openai_client

    # Fetch project id via metadata endpoint
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    try:
        project_id = urllib.request.urlopen(req).read().decode()
    except Exception as e:
        # Handle cases where the metadata server might not be available (e.g., local testing)
        print(f"Could not retrieve project ID from metadata server: {e}")
        project_id = None
        raise

    secret_name = "openai-api-key"

    # Fetch open ai key from GCP secret manager
    client = secretmanager.SecretManagerServiceClient()
    # Access the latest version of the secret
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    api_key = response.payload.data.decode("UTF-8")

    openai_client = OpenAI(api_key=api_key)
    return openai_client

@functions_framework.http
def get_embedding(request):
    try:
        request_json = request.get_json()
        calls = request_json['calls']

        # The protocol sends a list of lists: [['text1'], ['text2']]
        # We flatten this to a simple list of strings for the OpenAI API: ['text1', 'text2']
        # We filter out None values to prevent API errors
        texts = [call[0] for call in calls if call is not None and call[0] is not None]

        # If the batch is empty (e.g., all inputs were NULL), return empty replies
        if not texts:
            # We must return a list of None matching the input length
            return jsonify({ "replies": [None] * len(calls) })

        # Initialize client
        client = get_openai_client()

        # Call OpenAI (Batch Request)
        # We send all texts in one HTTP request for performance
        resp = client.embeddings.create(
            input=texts,
            model="text-embedding-3-small"
        )

        # Map OpenAI results back to the original order
        # OpenAI returns a list of data objects with an 'index' field
        embeddings_map = {item.index: item.embedding for item in resp.data}

        # Construct the replies list to match the order of 'calls'
        replies = []

        # We need a secondary counter for the valid texts we sent to OpenAI
        valid_text_index = 0

        for call in calls:
            if call is not None and call[0] is not None:
                # If this row had valid text, retrieve its result from the map
                if valid_text_index in embeddings_map:
                    replies.append(embeddings_map[valid_text_index])
                else:
                    replies.append(None)
                valid_text_index += 1
            else:
                # If the input was NULL, the result is NULL
                replies.append(None)

        # Return using jsonify as per the protocol
        return jsonify( { "replies": replies } )

    except Exception as e:
        # Return error message with 400 status code as per protocol
        return jsonify( { "errorMessage": str(e) } ), 400

```


4. **Deploy the Function**:
Replace `YOUR_REGION` with your desired Google Cloud region (e.g., `us-central1`). Make sure that you run the command below from the folder containing `main.py`.
```bash
gcloud functions deploy spanner-openai-func \
  --gen2 \
  --runtime=python310 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=get_embedding \
  --trigger-http \
  --no-allow-unauthenticated

gcloud run deploy spanner-openai-func \
  --source . \
  --function get_embedding \
  --region YOUR_REGION \
  --no-allow-unauthenticated

```

NOTE: You can also [deploy your Cloud Run Function via the GCP console](https://docs.cloud.google.com/run/docs/quickstarts/functions/deploy-functions-console):

![ss_crf_deploy](simple-udf/images/deploy_crf.png)

5. **Grant Permissions**:
The Cloud Run service account needs permission to access the secret we created in Step 1.
```bash
# Get the Service Account email for the deployed function
SERVICE_ACCOUNT=$(gcloud run services describe spanner-openai-func --region YOUR_REGION --format="value(spec.template.spec.serviceAccountName)")

# Grant Secret Accessor role
gcloud secrets add-iam-policy-binding openai-api-key \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor"

```

*If* the Cloud Run Function is in a different GCP project from the Spanner instance in question, you also need to ensure that the Spanner service account is granted the Spanner Service Agent role on the GCP project hosting the Cloud Run Function.

```bash
export SOURCE_PROJECT_NUMBER=$(gcloud projects describe [SOURCE_PROJECT_ID] --format="value(projectNumber)")

export SPANNER_P4SA="service-${SOURCE_PROJECT_NUMBER}@gcp-sa-spanner.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding [TARGET_PROJECT_ID] --member="serviceAccount:${SPANNER_P4SA}" --role="roles/spanner.serviceAgent"
```

The SOURCE_PROJECT_NUMBER is the project where Spanner instance is hosted.


---

## Step 3: Configure Spanner Schema & Remote UDF

Now we define the database schema and register the remote function. Before you can run the DDL statements and SQL queries below, pick a Spanner instance and database in your environment.

1. **Create the Table**:
This table stores the source text and the resulting embedding vector.
```sql
CREATE TABLE snippets (
  id INT64,
  text STRING(MAX),
  embedding ARRAY<FLOAT32>
) PRIMARY KEY(id);

```


2. **Create the Remote Function**:
This DDL statement registers the Cloud Run endpoint as a function callable from SQL.
* Replace `YOUR_CLOUD_RUN_URL` with the URL output from the deploy command in Step 2. This URL is of the form 'https://[YOUR_REGION]-[YOUR_PROJECT_ID].cloudfunctions.net/[YOUR_FUNCTION_NAME]'
* Adjust the `max_batching_rows` if you want to control how many rows Spanner sends to Python at once.

Remote functions in Spanner can only be created with a named schame. So let's create a named schema first:

```sql
CREATE SCHEMA udfs;

```

Please note that you can choose a different name (other than 'udfs') for the named schema that your remote function will be placed into.


```sql
CREATE FUNCTION udfs.GetOpenAIEmbedding(text STRING)
RETURNS ARRAY<FLOAT32> NOT DETERMINISTIC
LANGUAGE REMOTE OPTIONS (
  endpoint = 'YOUR_CLOUD_RUN_URL',
  max_batching_rows = 20
);

```

Please make sure you replace 'YOUR_REGION', 'YOUR_PROJECT_ID' AND 'YOUR_FUNCTION_NAME' with the corresponding values from your environment.

---

## Step 4: Update Embeddings with SQL

You can now use the `GetOpenAIEmbedding` function directly in your SQL statements.

1. **Insert Sample Data**:
```sql
INSERT INTO snippets (id, text) VALUES 
(1, 'Spanner is a globally distributed database.'),
(2, 'OpenAI provides powerful LLM APIs.'),
(3, 'Remote UDFs allow calling external code from SQL.');

```


2. **Generate Embeddings**:
Run an `UPDATE` statement to compute embeddings for rows that don't have them yet.
```sql
UPDATE snippets 
SET embedding = udfs.GetOpenAIEmbedding(text)
WHERE text IS NOT NULL AND embedding IS NULL;

```


3. **Verify Results**:
```sql
SELECT id, text, embedding FROM snippets;

```

4. **Cosine distance query**:
```sql
SELECT 
  id, 
  text, 
  COSINE_DISTANCE(embedding, udfs.GetOpenAIEmbedding('distributed systems')) AS distance
FROM snippets
WHERE embedding IS NOT NULL
ORDER BY distance ASC
LIMIT 5;

```