import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(
    api_key=os.getenv('XAI_API_KEY'),
    base_url='https://api.x.ai/v1'
)

response = client.chat.completions.create(
    model='grok-4-fast-non-reasoning',
    temperature=0,
    messages=[{'role': 'user', 'content': 'Reply with exactly: handshake ok'}]
)

print(response.choices[0].message.content)
print(f"Model: {response.model}")
print(f"Tokens used: {response.usage.total_tokens}")
