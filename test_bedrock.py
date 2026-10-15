import os
from dotenv import load_dotenv
from anthropic import AnthropicBedrock

load_dotenv()

api_key = os.getenv("BEDROCK_API_KEY")
region = os.getenv("AWS_REGION")
model_id = os.getenv("BEDROCK_MODEL_ID")

print("Using region:", region)
print("Using model:", model_id)

client = AnthropicBedrock(
    api_key=api_key,
    aws_region=region,
)

response = client.messages.create(
    model=model_id,
    max_tokens=50,
    messages=[
        {"role": "user", "content": "Say hello in one short sentence."}
    ],
)

print("\n--- Claude's response ---")
print(response.content[0].text)