// As credenciais são lidas das variáveis de ambiente da Vercel
const apiKey = process.env.GEMINI_API_KEY;

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

        const payload = {
            contents: [{ parts: [{ text: userPrompt }] }],
            systemInstruction: { parts: [{ text: systemPrompt }] },
            generationConfig: {
                responseMimeType: "application/json",
                responseSchema: { type: "OBJECT", properties, required }
            }
        };

        const geminiResponse = await fetch(apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await geminiResponse.json();

        if (!geminiResponse.ok) {
            console.error('Gemini API Error:', data);
            return response.status(geminiResponse.status).json({ error: data.error?.message || 'Falha na API do Gemini' });
        }

        const candidateText = data.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!candidateText) {
             return response.status(500).json({ error: 'Resposta da API inválida.' });
        }
        
        return response.status(200).json(JSON.parse(candidateText));

    } catch (error) {
        console.error('Proxy Error:', error);
        return response.status(500).json({ error: 'Erro interno do servidor.' });
    }
}

