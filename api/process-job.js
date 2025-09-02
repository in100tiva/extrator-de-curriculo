import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

// Bloco de inicialização robusto
if (!getApps().length) {
    try {
        const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
        initializeApp({ credential: cert(serviceAccount) });
    } catch (e) {
        console.error("ERRO CRÍTICO: Falha na inicialização do Firebase Admin em process-job.", e);
        throw e;
    }
}
const db = getFirestore();

// Função que executa o trabalho em segundo plano
async function processJobInBackground(jobId, userId, host) {
    try {
        console.log(`[PROCESS-JOB ${jobId}] Iniciando processamento...`);
        const jobRef = db.collection('processing_queue').doc(jobId);
        const jobDoc = await jobRef.get();

        // Se o job não for válido, aciona o próximo para não quebrar a cadeia
        if (!jobDoc.exists || jobDoc.data().status !== 'pending') {
            console.log(`[PROCESS-JOB ${jobId}] Job não encontrado ou já processado. Verificando próximo...`);
            await triggerNextJob(userId, host);
            return;
        }

        const jobData = jobDoc.data();
        await jobRef.update({ status: 'processing', startedAt: Timestamp.now() });

        const result = await callGeminiAPI(jobData.text, jobData.selectedFields);
        if (result.success) {
            await jobRef.update({ status: 'completed', finishedAt: Timestamp.now(), result: result.data });
            console.log(`[PROCESS-JOB ${jobId}] Concluído com sucesso.`);
        } else {
            await jobRef.update({ status: 'failed', finishedAt: Timestamp.now(), error: result.error });
            console.error(`[PROCESS-JOB ${jobId}] Falhou: ${result.error}`);
        }

        // Aciona o próximo job como a última etapa
        await triggerNextJob(userId, host);

    } catch (error) {
        console.error(`[PROCESS-JOB ${jobId}] Erro inesperado:`, error);
        // Tenta marcar o job como falho para que a lógica autocorretiva possa pegá-lo
        await db.collection('processing_queue').doc(jobId).update({ status: 'failed', error: 'Erro interno do worker.' }).catch(() => {});
    }
}

// Função handler principal, que responde imediatamente
export default function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).send('Method Not Allowed');
    }

    const { jobId, userId } = request.body;
    if (!jobId || !userId) {
        return response.status(400).send('Job ID and User ID are required.');
    }

    // **CORREÇÃO PRINCIPAL**
    // Responde IMEDIATAMENTE para o chamador (seja o 'start-processing' ou o job anterior)
    response.status(202).send(`Accepted job ${jobId}.`);

    // Inicia o processamento em segundo plano sem esperar que ele termine
    processJobInBackground(jobId, userId, request.headers.host);
}


async function triggerNextJob(userId, host) {
    const nextSnapshot = await db.collection('processing_queue')
        .where('userId', '==', userId)
        .where('status', '==', 'pending')
        .orderBy('createdAt')
        .limit(1)
        .get();

    if (!nextSnapshot.empty) {
        const nextJobId = nextSnapshot.docs[0].id;
        console.log(`[TRIGGER] Próximo job encontrado: ${nextJobId}. Acionando...`);
        
        try {
            const protocol = host.includes('localhost') ? 'http' : 'https';
            // Usa await para garantir que a requisição seja despachada
            await fetch(`${protocol}://${host}/api/process-job`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ jobId: nextJobId, userId: userId })
            });
        } catch (err) {
            console.error(`[TRIGGER] Erro ao acionar o próximo job ${nextJobId}:`, err);
        }
    } else {
        console.log(`[TRIGGER] Fila para ${userId} finalizada.`);
    }
}

async function callGeminiAPI(text, selectedFields) {
    const apiKey = process.env.GEMINI_API_KEY;
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
    
    try {
        const response = await fetch(apiUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        if (!response.ok) {
            const errorBody = await response.json();
            throw new Error(`API retornou status ${response.status}: ${errorBody.error?.message || 'Erro desconhecido'}`);
        }
        const result = await response.json();
        const candidate = result.candidates?.[0];
        if (candidate?.content?.parts?.[0]?.text) {
            return { success: true, data: JSON.parse(candidate.content.parts[0].text) };
        }
        throw new Error('Resposta da API inválida ou sem texto.');
    } catch (error) {
        return { success: false, error: error.message };
    }
}

