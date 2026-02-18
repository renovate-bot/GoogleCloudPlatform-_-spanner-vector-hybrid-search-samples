import functions_framework
from flask import jsonify
from google.cloud import secretmanager
from openai import OpenAI
import urllib.request

# Global client to reuse across warm instances
openai_client = None
project_id = None
secret_name = "openai-api-key"
model_name = "text-embedding-3-small"

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
            model=model_name
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