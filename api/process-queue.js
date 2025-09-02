import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

// --- INSTRUÇÕES IMPORTANTES ---
// 1. No seu projeto Firebase, vá em "Configurações do projeto" -> "Contas de serviço".
// 2. Clique em "Gerar nova chave privada" e baixe o arquivo JSON.
// 3. Copie o conteúdo COMPLETO desse arquivo JSON.
// 4. Na Vercel, vá nas configurações do seu projeto -> "Environment Variables".
// 5. Crie uma nova variável de ambiente chamada: GOOGLE_SERVICE_ACCOUNT_KEY
// 6. Cole o conteúdo do arquivo JSON como o valor dessa variável.
// --------------------------------

// Inicializa o Firebase Admin SDK
const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);

initializeApp({
  credential: cert(serviceAccount)
});

const db = getFirestore();

// --- LÓGICA DO WORKER ---
export default async function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).send('Method Not Allowed');
    }
    
    try {
        const queueRef = db.collection('processing_queue');
        
        // Verifica se já existe um job em processamento para evitar concorrência
        const processingSnapshot = await queueRef.where('status', '==', 'processing').limit(1).get();
        if (!processingSnapshot.empty) {
            return response.status(200).send('Worker is already busy.');
        }

        // Pega o job pendente mais antigo da fila
        const snapshot = await queueRef.where('status', '==', 'pending').orderBy('createdAt').limit(1).get();

        if (snapshot.empty) {
            return response.status(200).send('Queue is empty.');
        }

        const jobDoc = snapshot.docs[0];
        const jobId = jobDoc.id;
        const jobData = jobDoc.data();

        // Marca o job como "em processamento"
        await jobDoc.ref.update({ status: 'processing', startedAt: Timestamp.now() });

        // Chama a API do Gemini (aqui fica a lógica de extração)
        const result = await callGeminiAPI(jobData.text, jobData.selectedFields);

        if (result.success) {
            await jobDoc.ref.update({ status: 'completed', finishedAt: Timestamp.now(), result: result.data });
        } else {
            await jobDoc.ref.update({ status: 'failed', finishedAt: Timestamp.now(), error: result.error });
        }

        response.status(200).send(`Job ${jobId} processed.`);

    } catch (error) {
        console.error('Error processing queue:', error);
        // Tenta marcar o job como falho se possível, para não ficar travado
        if (jobId) {
             await db.collection('processing_queue').doc(jobId).update({ status: 'failed', error: 'Worker uncaught error' });
        }
        response.status(500).send('Internal Server Error');
    }
}


async function callGeminiAPI(text, selectedFields) {
    // --- INSTRUÇÕES IMPORTANTES ---
    // Na Vercel, crie uma variável de ambiente chamada GEMINI_API_KEY com a sua chave.
    // --------------------------------
    const apiKey = process.env.GEMINI_API_KEY;
    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();

    // O prompt do sistema é o mesmo de antes
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
        const response = await fetch(apiUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        if (!response.ok) {
            throw new Error(`API returned status ${response.status}`);
        }
        const result = await response.json();
        const candidate = result.candidates?.[0];
        if (candidate?.content?.parts?.[0]?.text) {
            return { success: true, data: JSON.parse(candidate.content.parts[0].text) };
        }
        throw new Error('Resposta da API inválida.');
    } catch (error) {
        console.error("Gemini API call failed:", error.message);
        return { success: false, error: error.message };
    }
}
