import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

// As credenciais são lidas das variáveis de ambiente da Vercel
const serviceAccountKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
const apiKey = process.env.GEMINI_API_KEY;

// Verificação robusta que PARA a execução se as chaves não existirem
if (!serviceAccountKey || !apiKey) {
    const errorMsg = "ERRO CRÍTICO: Variáveis de ambiente GOOGLE_SERVICE_ACCOUNT_KEY ou GEMINI_API_KEY não estão configuradas corretamente na Vercel.";
    console.error(errorMsg);
    throw new Error(errorMsg);
}

let serviceAccount;
try {
    serviceAccount = JSON.parse(serviceAccountKey);
} catch (e) {
    const errorMsg = "ERRO CRÍTICO: O conteúdo da variável GOOGLE_SERVICE_ACCOUNT_KEY não é um JSON válido.";
    console.error(errorMsg, e);
    throw new Error(errorMsg);
}

// Evita reinicialização do app em cada chamada (otimização da Vercel)
if (!getApps().length) {
    initializeApp({
        credential: cert(serviceAccount)
    });
}

const db = getFirestore();

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
        let currentJobRef = jobDoc.ref;

        try {
            await currentJobRef.update({ status: 'processing', startedAt: Timestamp.now() });
            const result = await callGeminiAPI(jobData.text, jobData.selectedFields);

            if (result.success) {
                await currentJobRef.update({ status: 'completed', finishedAt: Timestamp.now(), result: result.data });
                console.log(`Job ${jobId} concluído com sucesso.`);
            } else {
                await currentJobRef.update({ status: 'failed', finishedAt: Timestamp.now(), error: result.error });
                console.error(`Job ${jobId} falhou: ${result.error}`);
            }
        } catch (error) {
            console.error(`Erro inesperado no worker para o job ${jobId}:`, error);
            try {
                await currentJobRef.update({ status: 'failed', error: 'Erro interno do worker.' });
            } catch (updateError) {
                console.error(`Não foi possível atualizar o status do job ${jobId} para falho.`, updateError);
            }
        }
        await sleep(1500); // Pausa aumentada para 1.5s para respeitar a API
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
            console.error("Gemini API Error Body:", errorBody);
            throw new Error(`API retornou status ${response.status}: ${errorBody.error?.message || 'Erro desconhecido'}`);
        }
        const result = await response.json();
        const candidate = result.candidates?.[0];
        if (candidate?.content?.parts?.[0]?.text) {
            return { success: true, data: JSON.parse(candidate.content.parts[0].text) };
        }
        throw new Error('Resposta da API inválida ou sem texto.');
    } catch (error) {
        console.error("Gemini API call failed:", error.message);
        return { success: false, error: error.message };
    }
}

