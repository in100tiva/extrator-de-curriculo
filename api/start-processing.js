import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

// As credenciais são lidas das variáveis de ambiente da Vercel
const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY || '{}');
const apiKey = process.env.GEMINI_API_KEY;

// Evita reinicialização do app em cada chamada (otimização da Vercel)
if (!getApps().length) {
    initializeApp({
        credential: cert(serviceAccount)
    });
}

const db = getFirestore();

// Função para pausar a execução
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export default async function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).send('Method Not Allowed');
    }

    const { userId } = request.body;
    if (!userId) {
        return response.status(400).send('User ID is required.');
    }

    console.log(`Worker iniciado para o usuário: ${userId}`);

    // Retorna uma resposta imediata para o cliente, enquanto o worker continua em segundo plano
    response.status(202).send('Processing started.');

    // --- LÓGICA DO LOOP DO WORKER ---
    while (true) {
        const queueRef = db.collection('processing_queue');
        const snapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt')
            .limit(1)
            .get();

        if (snapshot.empty) {
            console.log(`Fila vazia para o usuário ${userId}. Worker finalizando.`);
            break; // Sai do loop se não houver mais arquivos pendentes
        }

        const jobDoc = snapshot.docs[0];
        const jobId = jobDoc.id;
        const jobData = jobDoc.data();

        try {
            await jobDoc.ref.update({ status: 'processing', startedAt: Timestamp.now() });

            const result = await callGeminiAPI(jobData.text, jobData.selectedFields);

            if (result.success) {
                await jobDoc.ref.update({ status: 'completed', finishedAt: Timestamp.now(), result: result.data });
                console.log(`Job ${jobId} concluído com sucesso.`);
            } else {
                await jobDoc.ref.update({ status: 'failed', finishedAt: Timestamp.now(), error: result.error });
                console.error(`Job ${jobId} falhou: ${result.error}`);
            }
        } catch (error) {
            console.error(`Erro inesperado no worker para o job ${jobId}:`, error);
            await jobDoc.ref.update({ status: 'failed', error: 'Erro interno do worker.' });
        }

        // Pausa de 1 segundo para respeitar a API do Gemini
        await sleep(1000);
    }
}

async function callGeminiAPI(text, selectedFields) {
    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    const systemPrompt = `Você é um assistente de RH de elite...`; // (O mesmo prompt detalhado da versão anterior)
    const userPrompt = `Extraia as informações do seguinte texto de currículo:\n\n--- INÍCIO DO CURRÍCULO ---\n${text}\n--- FIM DO CURRÍCULO ---`;
    const properties = { nome: { type: "STRING" } };
    const required = ["nome"];
    if (selectedFields.includes('idade')) { properties.idade = { type: "NUMBER" }; required.push('idade'); }
    if (selectedFields.includes('email')) { properties.email = { type: "STRING" }; required.push('email'); }
    if (selectedFields.includes('contatos')) { properties.contatos = { type: "ARRAY", items: { type: "STRING" } }; required.push('contatos'); }

    const payload = { /* ... mesmo payload da versão anterior ... */ };

    try {
        const response = await fetch(apiUrl, { /* ... mesmo fetch da versão anterior ... */ });
        if (!response.ok) throw new Error(`API retornou status ${response.status}`);
        const result = await response.json();
        const candidate = result.candidates?.[0];
        if (candidate?.content?.parts?.[0]?.text) {
            return { success: true, data: JSON.parse(candidate.content.parts[0].text) };
        }
        throw new Error('Resposta da API inválida.');
    } catch (error) {
        return { success: false, error: error.message };
    }
}
