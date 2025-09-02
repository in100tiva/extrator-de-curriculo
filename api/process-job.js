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

// TIMEOUT REDUZIDO para evitar limite de 10s da Vercel gratuita
const PROCESSING_TIMEOUT = 8000; // 8 segundos
const MAX_RETRIES = 2;

async function triggerNextJob(userId, host) {
    try {
        const nextSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt')
            .limit(1)
            .get();

        if (!nextSnapshot.empty) {
            const nextJobId = nextSnapshot.docs[0].id;
            console.log(`[TRIGGER] Próximo job encontrado: ${nextJobId}. Acionando...`);
            
            // Disparo assíncrono sem aguardar resposta para evitar timeouts
            const protocol = host.includes('localhost') ? 'http' : 'https';
            fetch(`${protocol}://${host}/api/process-job`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ jobId: nextJobId, userId: userId })
            }).catch(err => {
                console.error(`[TRIGGER] Erro ao acionar o próximo job ${nextJobId}:`, err);
            });
        } else {
            console.log(`[TRIGGER] Fila para ${userId} finalizada.`);
        }
    } catch (error) {
        console.error('[TRIGGER] Erro ao buscar próximo job:', error);
    }
}

export default async function handler(request, response) {
    if (request.method !== 'POST') return response.status(405).send('Method Not Allowed');

    const { jobId, userId } = request.body;
    if (!jobId || !userId) return response.status(400).send('Job ID and User ID are required.');

    // Timeout de segurança para toda a função
    const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Function timeout')), PROCESSING_TIMEOUT);
    });

    try {
        console.log(`[PROCESS-JOB ${jobId}] Iniciando processamento...`);
        const jobRef = db.collection('processing_queue').doc(jobId);
        
        // Verificação e update atômico
        const processResult = await Promise.race([
            processJobWithRetry(jobRef, jobId, userId, request.headers.host),
            timeoutPromise
        ]);

        return response.status(200).json(processResult);

    } catch (error) {
        console.error(`[PROCESS-JOB ${jobId}] Erro inesperado:`, error);
        
        // Marca job como failed em caso de timeout ou erro
        try {
            await db.collection('processing_queue').doc(jobId).update({ 
                status: 'failed', 
                finishedAt: Timestamp.now(),
                error: error.message || 'Timeout ou erro interno',
                retryCount: 0
            });
            
            // Tenta acionar próximo job mesmo em caso de falha
            await triggerNextJob(userId, request.headers.host);
        } catch (updateError) {
            console.error(`[PROCESS-JOB ${jobId}] Erro ao atualizar status de falha:`, updateError);
        }

        return response.status(200).json({ 
            success: false, 
            error: 'Job processado com falha, próximo job acionado' 
        });
    }
}

async function processJobWithRetry(jobRef, jobId, userId, host) {
    const jobDoc = await jobRef.get();
    
    if (!jobDoc.exists) {
        console.log(`[PROCESS-JOB ${jobId}] Job não encontrado. Verificando próximo...`);
        await triggerNextJob(userId, host);
        return { success: false, error: 'Job not found' };
    }

    const jobData = jobDoc.data();
    const retryCount = jobData.retryCount || 0;

    // Se job não está pending, verifica se deve ser reprocessado
    if (jobData.status !== 'pending') {
        if (jobData.status === 'processing' && retryCount < MAX_RETRIES) {
            // Reset job travado
            console.log(`[PROCESS-JOB ${jobId}] Resetando job travado (tentativa ${retryCount + 1})`);
            await jobRef.update({ 
                status: 'pending',
                retryCount: retryCount + 1
            });
        } else {
            console.log(`[PROCESS-JOB ${jobId}] Job já processado ou excedeu tentativas. Verificando próximo...`);
            await triggerNextJob(userId, host);
            return { success: false, error: 'Job already processed or max retries exceeded' };
        }
    }

    // Marca como processing
    await jobRef.update({ 
        status: 'processing', 
        startedAt: Timestamp.now(),
        retryCount: retryCount
    });

    try {
        // Processamento com timeout reduzido
        const result = await Promise.race([
            callGeminiAPI(jobData.text, jobData.selectedFields),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Gemini API timeout')), 6000)
            )
        ]);

        if (result.success) {
            await jobRef.update({ 
                status: 'completed', 
                finishedAt: Timestamp.now(), 
                result: result.data 
            });
            console.log(`[PROCESS-JOB ${jobId}] Concluído com sucesso.`);
        } else {
            throw new Error(result.error);
        }

        // Aciona próximo job
        await triggerNextJob(userId, host);
        
        return { success: true, message: `Job ${jobId} processed successfully` };

    } catch (apiError) {
        console.error(`[PROCESS-JOB ${jobId}] Falha na API:`, apiError);
        
        if (retryCount < MAX_RETRIES) {
            // Reset para nova tentativa
            await jobRef.update({ 
                status: 'pending',
                retryCount: retryCount + 1
            });
            console.log(`[PROCESS-JOB ${jobId}] Agendado para nova tentativa (${retryCount + 1}/${MAX_RETRIES})`);
        } else {
            // Marca como failed definitivamente
            await jobRef.update({ 
                status: 'failed', 
                finishedAt: Timestamp.now(), 
                error: apiError.message || 'API error after retries'
            });
        }

        // Sempre aciona próximo job
        await triggerNextJob(userId, host);
        return { success: false, error: 'API processing failed' };
    }
}

async function callGeminiAPI(text, selectedFields) {
    const apiKey = process.env.GEMINI_API_KEY;
    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    
    // Texto reduzido para acelerar processamento
    const truncatedText = text.length > 3000 ? text.substring(0, 3000) : text;
    
    const systemPrompt = `Você é um assistente de RH de elite, focado em extrair dados de textos de currículos com alta precisão e RAPIDEZ.

REGRAS CRÍTICAS DE EXTRAÇÃO:
1.  **NOME**: Extraia o nome completo que geralmente aparece no topo. SEMPRE formate o nome para que a primeira letra de cada palavra seja maiúscula, exceto para conectivos como "de", "da", "do", "dos" que devem ser minúsculos.
2.  **IDADE**: Procure por um número seguido diretamente pela palavra "anos" (ex: "37 anos") OU calcule pela data de nascimento (ano atual: ${currentYear}). Se não encontrar, retorne 0.
3.  **CONTATOS**: Extraia TODOS os números de telefone válidos, formatando para (DD) 9 XXXX-XXXX ou (DD) XXXX-XXXX.
4.  **EMAIL**: Encontre o e-mail que sempre contém "@".
5.  **SAÍDA**: Responda APENAS com o objeto JSON, sem texto extra.`;
    
    const userPrompt = `Extraia as informações do seguinte texto de currículo:\n\n${truncatedText}`;
    
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
            responseSchema: { type: "OBJECT", properties, required },
            maxOutputTokens: 500, // Limite para acelerar
            temperature: 0.1
        }
    };
    
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000); // 5s timeout
        
        const response = await fetch(apiUrl, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload),
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (!response.ok) {
            const errorBody = await response.json().catch(() => ({}));
            throw new Error(`API retornou status ${response.status}: ${errorBody.error?.message || 'Erro desconhecido'}`);
        }
        
        const result = await response.json();
        const candidate = result.candidates?.[0];
        
        if (candidate?.content?.parts?.[0]?.text) {
            return { success: true, data: JSON.parse(candidate.content.parts[0].text) };
        }
        
        throw new Error('Resposta da API inválida ou sem texto.');
        
    } catch (error) {
        if (error.name === 'AbortError') {
            return { success: false, error: 'Gemini API timeout' };
        }
        return { success: false, error: error.message };
    }
}