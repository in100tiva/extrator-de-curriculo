import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

let db;
let initError = null;

// --- INICIALIZAÇÃO ROBUSTA ---
// Este bloco de código é executado apenas uma vez quando a função é iniciada.
try {
    console.log("Lendo as variáveis de ambiente...");
    const serviceAccountKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
    const apiKey = process.env.GEMINI_API_KEY;

    if (!serviceAccountKey) throw new Error("Variável de ambiente 'GOOGLE_SERVICE_ACCOUNT_KEY' não encontrada.");
    if (!apiKey) throw new Error("Variável de ambiente 'GEMINI_API_KEY' não encontrada.");
    
    console.log("Variáveis lidas com sucesso.");

    let serviceAccount;
    try {
        serviceAccount = JSON.parse(serviceAccountKey);
    } catch (e) {
        throw new Error("Falha ao analisar o JSON da 'GOOGLE_SERVICE_ACCOUNT_KEY'. Verifique se o conteúdo foi copiado corretamente.");
    }

    if (!getApps().length) {
        console.log("Inicializando o Firebase Admin SDK...");
        initializeApp({
            credential: cert(serviceAccount)
        });
        console.log("Firebase Admin SDK inicializado com sucesso.");
    }
    db = getFirestore();

} catch (error) {
    // Armazena o erro de inicialização para que possa ser retornado em cada requisição
    console.error("ERRO CRÍTICO DURANTE A INICIALIZAÇÃO:", error.message);
    initError = error.message;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export default async function handler(request, response) {
    // Se a inicialização falhou, retorna um erro claro em todas as chamadas
    if (initError) {
        return response.status(500).json({ error: `Erro de configuração do servidor: ${initError}` });
    }
    if (request.method !== 'POST') {
        return response.status(405).send('Method Not Allowed');
    }

    const { userId } = request.body;
    if (!userId) {
        return response.status(400).send('User ID is required.');
    }

    console.log(`Worker iniciado para o usuário: ${userId}`);
    response.status(202).send('Processing started.');

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
            break; 
        }

        const jobDoc = snapshot.docs[0];
        const jobId = jobDoc.id;
        const jobData = jobDoc.data();

        try {
            await jobDoc.ref.update({ status: 'processing', startedAt: Timestamp.now() });
            const result = await callGeminiAPI(jobData.text, jobData.selectedFields);

            if (result.success) {
                await jobDoc.ref.update({ status: 'completed', finishedAt: Timestamp.now(), result: result.data });
            } else {
                await jobDoc.ref.update({ status: 'failed', finishedAt: Timestamp.now(), error: result.error });
            }
        } catch (error) {
            await jobDoc.ref.update({ status: 'failed', error: 'Erro interno do worker.' });
        }
        await sleep(1500);
    }
}

async function callGeminiAPI(text, selectedFields) {
    const apiKey = process.env.GEMINI_API_KEY;
    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    
    const systemPrompt = `Você é um assistente de RH...`; // Prompt completo omitido para brevidade
    const userPrompt = `Extraia as informações do seguinte texto de currículo...\n${text}`;
    
    const properties = { nome: { type: "STRING" } };
    const required = ["nome"];
    if (selectedFields.includes('idade')) { properties.idade = { type: "NUMBER" }; required.push('idade'); }
    if (selectedFields.includes('email')) { properties.email = { type: "STRING" }; required.push('email'); }
    if (selectedFields.includes('contatos')) { properties.contatos = { type: "ARRAY", items: { type: "STRING" } }; required.push('contatos'); }

    const payload = { /* ... */ }; // Payload completo omitido para brevidade

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

