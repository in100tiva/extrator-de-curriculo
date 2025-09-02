// As credenciais são lidas das variáveis de ambiente da Vercel
const apiKey = process.env.GEMINI_API_KEY;

// Função para pausar a execução
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Função handler da Vercel
export default async function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).json({ error: 'Method Not Allowed' });
    }

    if (!apiKey) {
        return response.status(500).json({ error: 'Chave da API não configurada no servidor.' });
    }

    try {
        const { text, selectedFields } = request.body;
        if (!text || !selectedFields) {
            return response.status(400).json({ error: 'Texto e campos selecionados são obrigatórios.' });
        }

        const model = "gemini-2.5-flash-preview-05-20";
        const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
        
        // --- LÓGICA DE RETENTATIVA APRIMORADA ---
        let geminiResponse;
        let lastError;
        const maxRetries = 7; // Aumentado de 5 para 7
        let delay = 2000; // Aumentado de 1000ms para 2000ms

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            geminiResponse = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(createPayload(text, selectedFields))
            });

            if (geminiResponse.ok) {
                const data = await geminiResponse.json();
                const candidateText = data.candidates?.[0]?.content?.parts?.[0]?.text;
                if (candidateText) {
                    return response.status(200).json(JSON.parse(candidateText));
                }
                lastError = 'Resposta da API bem-sucedida, mas com conteúdo inválido.';
                // Pausa antes de tentar novamente mesmo em caso de sucesso com corpo vazio
                await sleep(delay); 
                continue;
            } 
            
            if (geminiResponse.status === 429) {
                lastError = `Limite de requisições da API atingido (status ${geminiResponse.status}).`;
                const jitter = Math.random() * 1000; // Adiciona variação de até 1s
                const waitTime = delay + jitter;
                console.log(`${lastError} Tentativa ${attempt}/${maxRetries}. Aguardando ${(waitTime / 1000).toFixed(1)}s...`);
                await sleep(waitTime);
                delay *= 2; // Dobra o tempo de espera base
                continue; // Tenta novamente
            } 
            
            // Para outros erros (400, 500, etc.), falha imediatamente
            const errorData = await geminiResponse.json();
            console.error('Gemini API Error:', errorData);
            return response.status(geminiResponse.status).json({ error: errorData.error?.message || 'Falha na API do Gemini' });
        }
        
        // Se todas as tentativas falharem
        console.error('Falha ao chamar a API do Gemini após múltiplas tentativas:', lastError);
        return response.status(500).json({ error: `Falha após ${maxRetries} tentativas. Último erro: ${lastError}` });

    } catch (error) {
        console.error('Proxy Error:', error);
        return response.status(500).json({ error: 'Erro interno do servidor.' });
    }
}

function createPayload(text, selectedFields) {
    const currentYear = new Date().getFullYear();
    const systemPrompt = `Você é um assistente de RH de elite, focado em extrair dados de textos de currículos com alta precisão.

REGRAS CRÍTICAS DE EXTRAÇÃO:
1.  **NOME**: Extraia o nome completo que geralmente aparece no topo. SEMPRE formate o nome para que a primeira letra de cada palavra seja maiúscula, exceto para conectivos como "de", "da", "do", "dos" que devem ser minúsculos. Exemplo: "RAQUEL DE OLIVEIRA SILVA" deve se tornar "Raquel de Oliveira Silva".
2.  **IDADE**:
    - PRIMEIRO, procure por um número seguido diretamente pela palavra "anos" (ex: "37 anos").
    - SE NÃO ENCONTRAR, procure por uma data de nascimento (DD/MM/AAAA) e calcule a idade (ano atual: ${currentYear}).
    - Se nenhum método funcionar, retorne 0.
3.  **CONTATOS**:
    - Extraia TODOS os números de telefone. Preste atenção em números próximos a "WhatsApp", "Celular", "Fone".
    - Ignore outros números que não sejam telefones (ex: datas de experiência).
4.  **EMAIL**: Encontre o e-mail, que sempre contém "@".
5.  **FORMATAÇÃO DE CONTATO**: Todos os números de telefone devem ser formatados para o padrão (DD) 9 XXXX-XXXX. Se não tiver 9 dígitos no corpo, use (DD) XXXX-XXXX.
6.  **SAÍDA**: Responda APENAS com o objeto JSON, sem nenhum texto extra. Siga o esquema JSON rigorosamente.`;
    
    const userPrompt = `Extraia as informações do seguinte texto de currículo:\n\n--- INÍCIO DO CURRÍCULO ---\n${text}\n--- FIM DO CURRÍCULO ---`;
    
    const properties = { nome: { type: "STRING" } };
    const required = ["nome"];
    if (selectedFields.includes('idade')) { properties.idade = { type: "NUMBER" }; required.push('idade'); }
    if (selectedFields.includes('email')) { properties.email = { type: "STRING" }; required.push('email'); }
    if (selectedFields.includes('contatos')) { properties.contatos = { type: "ARRAY", items: { type: "STRING" } }; required.push('contatos'); }

    return {
        contents: [{ parts: [{ text: userPrompt }] }],
        systemInstruction: { parts: [{ text: systemPrompt }] },
        generationConfig: {
            responseMimeType: "application/json",
            responseSchema: { type: "OBJECT", properties, required }
        }
    };
}

