import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

// Bloco de inicialização robusto
if (!getApps().length) {
    try {
        const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
        initializeApp({ credential: cert(serviceAccount) });
    } catch (e) {
        console.error("ERRO CRÍTICO: Falha na inicialização do Firebase Admin.", e);
        throw e;
    }
}
const db = getFirestore();

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export default async function handler(request, response) {
    if (request.method !== 'POST') return response.status(405).send('Method Not Allowed');
    
    const { userId } = request.body;
    if (!userId) return response.status(400).send('User ID is required.');

    try {
        console.log(`[WORKER] Iniciador Autocorretivo para o usuário ${userId}`);
        const queueRef = db.collection('processing_queue');

        // Lógica Autocorretiva: Reseta jobs que possam ter travado
        const stuckJobsSnapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'processing')
            .get();

        if (!stuckJobsSnapshot.empty) {
            console.log(`[WORKER] ${stuckJobsSnapshot.size} job(s) travado(s) encontrado(s). Resetando para 'pending'...`);
            const batch = db.batch();
            stuckJobsSnapshot.docs.forEach(doc => {
                batch.update(doc.ref, { status: 'pending' });
            });
            await batch.commit();
            console.log(`[WORKER] Jobs travados resetados.`);
        }

        // Responde imediatamente que o processo de verificação foi feito
        // e o processamento em lote vai começar.
        response.status(202).send('Processing initiated.');

        // --- LÓGICA DO PROCESSADOR EM LOTE ---
        const MAX_JOBS_PER_RUN = 4; // Limite para evitar timeout
        for (let i = 0; i < MAX_JOBS_PER_RUN; i++) {
            const snapshot = await queueRef
                .where('userId', '==', userId)
                .where('status', '==', 'pending')
                .orderBy('createdAt')
                .limit(1)
                .get();

            if (snapshot.empty) {
                console.log(`[WORKER] Fila para ${userId} está vazia. Finalizando lote.`);
                break; // Sai do loop se não houver mais jobs
            }

            const jobDoc = snapshot.docs[0];
            const jobId = jobDoc.id;
            const jobData = jobDoc.data();

            try {
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
                 console.error(`[JOB ${jobId}] Erro inesperado:`, error);
                 await jobDoc.ref.update({ status: 'failed', error: 'Erro interno do worker.' });
            }
            
            // Pausa para respeitar a API do Gemini
            await sleep(1500); 
        }

    } catch (error) {
        console.error(`[WORKER] Erro geral no handler para ${userId}:`, error);
        // Não enviamos resposta de erro aqui porque a resposta 202 já foi enviada.
        // O erro já foi logado para depuração.
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

