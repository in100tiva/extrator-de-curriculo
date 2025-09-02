import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

let db;
let initError = null;

// --- INICIALIZAÇÃO ROBUSTA COM LOGS DE DEBUG APRIMORADOS ---
try {
    console.log("[DEBUG] Iniciando a inicialização da função...");

    console.log("[DEBUG] Lendo as variáveis de ambiente...");
    const serviceAccountKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
    const apiKey = process.env.GEMINI_API_KEY;

    // Novos logs para verificar se as variáveis estão sendo lidas
    console.log(`[DEBUG] Tamanho da GOOGLE_SERVICE_ACCOUNT_KEY: ${serviceAccountKey ? serviceAccountKey.length : 0}`);
    console.log(`[DEBUG] GEMINI_API_KEY existe: ${!!apiKey}`);

    if (!serviceAccountKey) throw new Error("Variável de ambiente 'GOOGLE_SERVICE_ACCOUNT_KEY' não encontrada.");
    if (!apiKey) throw new Error("Variável de ambiente 'GEMINI_API_KEY' não encontrada.");
    
    console.log("[DEBUG] Variáveis de ambiente lidas com sucesso.");

    let serviceAccount;
    try {
        console.log("[DEBUG] Analisando o JSON da chave de serviço...");
        serviceAccount = JSON.parse(serviceAccountKey);
        console.log("[DEBUG] JSON da chave de serviço analisado com sucesso.");
    } catch (e) {
        throw new Error("Falha ao analisar o JSON da 'GOOGLE_SERVICE_ACCOUNT_KEY'. Verifique se o conteúdo foi copiado corretamente.");
    }

    if (!getApps().length) {
        console.log("[DEBUG] Nenhuma aplicação Firebase encontrada. Inicializando o Firebase Admin SDK...");
        initializeApp({
            credential: cert(serviceAccount)
        });
        console.log("[DEBUG] Firebase Admin SDK inicializado com sucesso.");
    } else {
        console.log("[DEBUG] Aplicação Firebase já existe. Reutilizando a instância.");
    }
    
    db = getFirestore();
    console.log("[DEBUG] Conexão com o Firestore estabelecida.");

} catch (error) {
    console.error("ERRO CRÍTICO DURANTE A INICIALIZAÇÃO:", error.message);
    initError = error; // Armazena o objeto de erro completo
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export default async function handler(request, response) {
    // A verificação de erro agora loga a mensagem e a envia na resposta
    if (initError) {
        console.error("[HANDLER] Erro de inicialização detectado:", initError.message);
        return response.status(500).json({ 
            error: `Erro de configuração do servidor.`,
            details: initError.message 
        });
    }

    if (request.method !== 'POST') {
        return response.status(405).send('Method Not Allowed');
    }

    const { userId } = request.body;
    if (!userId) {
        return response.status(400).send('User ID is required.');
    }

    console.log(`[HANDLER] Worker iniciado para o usuário: ${userId}`);
    response.status(202).send('Processing started.');

    let jobCount = 0;
    while (true) {
        const queueRef = db.collection('processing_queue');
        const snapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt')
            .limit(1)
            .get();

        if (snapshot.empty) {
            console.log(`[HANDLER] Fila vazia para o usuário ${userId} após processar ${jobCount} trabalho(s). Worker finalizando.`);
            break; 
        }

        jobCount++;
        const jobDoc = snapshot.docs[0];
        const jobId = jobDoc.id;
        const jobData = jobDoc.data();

        try {
            console.log(`[JOB ${jobId}] Iniciando processamento...`);
            await jobDoc.ref.update({ status: 'processing', startedAt: Timestamp.now() });
            const result = await callGeminiAPI(jobData.text, jobData.selectedFields);

            if (result.success) {
                await jobDoc.ref.update({ status: 'completed', finishedAt: Timestamp.now(), result: result.data });
                console.log(`[JOB ${jobId}] Concluído com sucesso.`);
            } else {
                await jobDoc.ref.update({ status: 'failed', finishedAt: Timestamp.now(), error: result.error });
                console.error(`[JOB ${jobId}] Falhou: ${result.error}`);
            }
        } catch (error) {
            console.error(`[JOB ${jobId}] Erro inesperado no worker:`, error);
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
    
    const systemPrompt = `Você é um assistente de RH de elite...`; // Omitido para brevidade
    
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
        const response = await fetch(apiUrl, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload) 
        });

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

