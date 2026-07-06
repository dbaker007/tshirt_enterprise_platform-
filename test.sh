uv run python -c "
import httpx, json
payload = {
    'model': 'llama3.1:8b',
    'messages': [
        {'role': 'user', 'content': 'Show me what is stuck under fraud review for Kentucky'}
    ],
    'tools': [{
        'type': 'function',
        'function': {
            'name': 'list_pending_holds',
            'description': 'Queries cluster shards.',
            'parameters': {
                'type': 'object',
                'properties': {
                    'state_code': {
                        'type': 'string',
                        'description': 'US State code filter'
                    }
                },
                'required': []
            }
        }
    }]
}
r = httpx.post('http://localhost:11434/api/chat', json=payload, timeout=10.0)
print(f'STATUS CODE: {r.status_code}')
print(f'RAW OLLAMA ERROR DETAILS: {r.text}')
"
