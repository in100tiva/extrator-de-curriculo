// api/proxy.js

export default async function handler(request, response) {
    // Apenas permite requisições do tipo POST
    if (request.method !== 'POST') {
        return response.status(405).json({ error: 'Method Not Allowed' });
    }

    // Pega o token da API do Gemini das variáveis de ambiente (forma segura)
    const apiKey = process.env.GEMINI_API_KEY;

    if (!apiKey) {
        return response.status(500).json({ error: 'API key not configured' });
    }

    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;

    try {
        // Envia a requisição para a API do Gemini usando o corpo da requisição que veio do frontend
        const geminiResponse = await fetch(apiUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(request.body),
        });

        const data = await geminiResponse.json();

        // Se a resposta da API do Gemini não for bem-sucedida, repassa o erro
        if (!geminiResponse.ok) {
            console.error('Gemini API Error:', data);
            return response.status(geminiResponse.status).json({ error: 'Failed to fetch data from Gemini API', details: data });
        }
        
        // Retorna a resposta da API do Gemini para o frontend
        return response.status(200).json(data);

    } catch (error) {
        console.error('Proxy Error:', error);
        return response.status(500).json({ error: 'Internal Server Error' });
    }
}
