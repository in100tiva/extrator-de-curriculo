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

export default async function handler(request, response) {
    if (request.method !== 'POST') return response.status(405).send('Method Not Allowed');

    const { jobId, userId } = request.body;
    if (!jobId || !userId) return response.status(400).send('Job ID and User ID are required.');

    try {
        console.log(`[PROCESS-JOB ${jobId}] Iniciando processamento...`);
        const jobRef = db.collection('processing_queue').doc(jobId);
        const jobDoc = await jobRef.get();

        if (!jobDoc.exists || jobDoc.data().status !== 'pending') {
            console.log(`[PROCESS-JOB ${jobId}] Job não encontrado, já processado ou inválido.`);
            // Mesmo se o job não for válido, procuramos o próximo para não quebrar a corrente
            await triggerNextJob(userId, request.headers.host);
            return response.status(200).send('Job already processed or invalid.');
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

        // Após terminar, aciona o próximo
        await triggerNextJob(userId, request.headers.host);
        
        response.status(200).send(`Job ${jobId} processed.`);

    } catch (error) {
        console.error(`[PROCESS-JOB ${jobId}] Erro inesperado:`, error);
        // Tenta marcar como falho mesmo em caso de erro
        await db.collection('processing_queue').doc(jobId).update({ status: 'failed', error: 'Erro interno do worker.' }).catch(() => {});
        response.status(500).send('Internal Server Error');
    }
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
        
        const protocol = host.includes('localhost') ? 'http' : 'https';
        fetch(`${protocol}://${host}/api/process-job`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ jobId: nextJobId, userId: userId })
        }).catch(err => console.error(`[TRIGGER] Erro ao acionar o próximo job ${nextJobId}:`, err));
    } else {
        console.log(`[TRIGGER] Fila para ${userId} finalizada.`);
    }
}


async function callGeminiAPI(text, selectedFields) {
    const apiKey = process.env.GEMINI_API_KEY;
    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    const systemPrompt = `Você é um assistente de RH de elite...`; // Omitido para brevidade
    const userPrompt = `Extraia as informações...`; // Omitido para brevidade
    const properties = { nome: { type: "STRING" } }; // Omitido para brevidade
    const required = ["nome"];
    if (selectedFields.includes('idade')) { /* ... */ }
    if (selectedFields.includes('email')) { /* ... */ }
    if (selectedFields.includes('contatos')) { /* ... */ }
    const payload = { /* ... */ }; // Omitido para brevidade
    
    // O corpo da função callGeminiAPI permanece o mesmo
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
