// api/process-batch.js - Nova API para processamento em lote otimizado
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

if (!getApps().length) {
    try {
        const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
        initializeApp({ credential: cert(serviceAccount) });
    } catch (e) {
        console.error("ERRO: Falha na inicialização do Firebase Admin.", e);
        throw e;
    }
}
const db = getFirestore();

export const config = {
    maxDuration: 10, // 10 segundos máximo na Vercel
};

export default async function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).json({ error: 'Method Not Allowed' });
    }

    const startTime = Date.now();
    const { userId, batchSize = 5 } = request.body; // Reduzido para 5 por batch
    
    if (!userId) {
        return response.status(400).json({ error: 'User ID is required' });
    }

    console.log(`[BATCH-PROCESSOR] Iniciando para ${userId}, batch size: ${batchSize}`);

    try {
        // 1. Buscar jobs pendentes (consulta simples)
        const pendingJobs = await findPendingJobs(userId, batchSize);
        
        if (pendingJobs.length === 0) {
            return response.status(200).json({ 
                success: true, 
                processed: 0,
                message: 'Nenhum job pendente encontrado'
            });
        }

        console.log(`[BATCH-PROCESSOR] ${pendingJobs.length} jobs encontrados`);

        // 2. Processar jobs em paralelo (Promise.all)
        const processPromises = pendingJobs.map(job => 
            processJobDirectly(job.id, job.data, userId)
        );

        // 3. Aguardar todos com timeout de 8 segundos
        const results = await Promise.allSettled(processPromises);
        
        const successful = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const failed = results.filter(r => r.status === 'rejected' || !r.value?.success).length;

        // 4. Disparar próximo batch se ainda há jobs pendentes
        const hasMoreJobs = await checkForMoreJobs(userId);
        if (hasMoreJobs) {
            // Fire-and-forget para próximo batch
            setTimeout(() => {
                fetch(`${getBaseUrl()}/api/process-batch`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ userId, batchSize })
                }).catch(err => console.error('[BATCH] Erro ao disparar próximo batch:', err));
            }, 2000); // 2 segundos de delay
        }

        const duration = Date.now() - startTime;
        console.log(`[BATCH-PROCESSOR] ✅ Concluído em ${duration}ms: ${successful} sucessos, ${failed} falhas`);

        return response.status(200).json({
            success: true,
            processed: successful,
            failed: failed,
            duration: duration,
            hasMoreJobs: hasMoreJobs
        });

    } catch (error) {
        console.error('[BATCH-PROCESSOR] Erro crítico:', error);
        return response.status(200).json({ // Sempre 200 para não travar
            success: false,
            error: error.message,
            processed: 0
        });
    }
}

// Busca jobs pendentes com consulta simples (sem índices complexos)
async function findPendingJobs(userId, limit) {
    try {
        const snapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .limit(limit)
            .get();

        return snapshot.docs.map(doc => ({
            id: doc.id,
            data: doc.data()
        }));
    } catch (error) {
        console.error('[FIND-JOBS] Erro:', error);
        return [];
    }
}

// Verifica se ainda há jobs para processar
async function checkForMoreJobs(userId) {
    try {
        const snapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .limit(1)
            .get();
        
        return !snapshot.empty;
    } catch (error) {
        return false;
    }
}

// Processa um job diretamente (sem HTTP calls)
async function processJobDirectly(jobId, jobData, userId) {
    try {
        console.log(`[DIRECT-PROCESS] Processando ${jobId}: ${jobData.fileName}`);

        // Marca como processando
        const jobRef = db.collection('processing_queue').doc(jobId);
        await jobRef.update({
            status: 'processing',
            startedAt: Timestamp.now()
        });

        // Processa com Gemini
        let result = await processWithGeminiOptimized(jobData.text, jobData.selectedFields);
        
        // Fallback se necessário
        if (!result.success) {
            result = extractWithSimpleFallback(jobData.text, jobData.selectedFields, jobData.fileName);
        }

        // Salva resultado
        if (result.success) {
            await jobRef.update({
                status: 'completed',
                finishedAt: Timestamp.now(),
                result: result.data
            });
        } else {
            await jobRef.update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: result.error
            });
        }

        return result;

    } catch (error) {
        console.error(`[DIRECT-PROCESS] Erro no job ${jobId}:`, error);
        
        // Marca como falha
        try {
            await db.collection('processing_queue').doc(jobId).update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: error.message
            });
        } catch (updateError) {
            console.error('[DIRECT-PROCESS] Erro ao marcar falha:', updateError);
        }

        return { success: false, error: error.message };
    }
}

// Processamento Gemini otimizado
async function processWithGeminiOptimized(text, selectedFields) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        return { success: false, error: 'API Key não configurada' };
    }

    try {
        // Texto mais curto para acelerar
        const shortText = text.length > 1500 ? text.substring(0, 1500) : text;
        
        // Prompt mais simples e direto
        const prompt = `Extraia do currículo:\n\n${shortText}\n\nResposta (apenas JSON):`;
        
        // Schema mínimo
        const properties = { nome: { type: "STRING" } };
        if (selectedFields.includes('idade')) properties.idade = { type: "NUMBER" };
        if (selectedFields.includes('email')) properties.email = { type: "STRING" };
        if (selectedFields.includes('contatos')) properties.contatos = { type: "ARRAY", items: { type: "STRING" } };

        const payload = {
            contents: [{ parts: [{ text: prompt }] }],
            generationConfig: {
                responseMimeType: "application/json",
                responseSchema: { type: "OBJECT", properties },
                maxOutputTokens: 200, // Reduzido para acelerar
                temperature: 0
            }
        };

        // Timeout agressivo de 3 segundos
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);
        
        const response = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key=${apiKey}`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
                signal: controller.signal
            }
        );
        
        clearTimeout(timeoutId);

        if (!response.ok) {
            throw new Error(`API HTTP ${response.status}`);
        }

        const result = await response.json();
        const extracted = result.candidates?.[0]?.content?.parts?.[0]?.text;
        
        if (!extracted) {
            throw new Error('Resposta vazia da API');
        }

        return { success: true, data: JSON.parse(extracted) };

    } catch (error) {
        if (error.name === 'AbortError') {
            return { success: false, error: 'Timeout Gemini (3s)' };
        }
        return { success: false, error: `Gemini: ${error.message}` };
    }
}

// Fallback simples e rápido
function extractWithSimpleFallback(text, selectedFields, fileName) {
    const result = { nome: fileName.replace(/\.pdf$/i, '') || 'Nome não encontrado' };
    
    try {
        // Nome - busca padrão simples
        const nomeMatch = text.match(/^([A-ZÀÁÂÃÄ][a-zàáâãä\s]{5,40})/m);
        if (nomeMatch) {
            result.nome = nomeMatch[1].trim();
        }

        // Idade - busca "X anos"
        if (selectedFields.includes('idade')) {
            const idadeMatch = text.match(/(\d{2})\s*anos?\b/i);
            result.idade = idadeMatch ? parseInt(idadeMatch[1]) : 0;
        }

        // Email - busca @
        if (selectedFields.includes('email')) {
            const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            result.email = emailMatch ? emailMatch[1].toLowerCase() : '';
        }

        // Contatos - busca números
        if (selectedFields.includes('contatos')) {
            const phones = text.match(/(?:\(?\d{2}\)?\s*)?[9]?\d{4,5}[\s-]?\d{4}/g);
            result.contatos = phones ? phones.slice(0, 2) : []; // Máximo 2
        }

    } catch (error) {
        console.error('[FALLBACK] Erro:', error);
    }

    return { success: true, data: result };
}

// URL base otimizada
function getBaseUrl() {
    if (process.env.VERCEL_URL) {
        return `https://${process.env.VERCEL_URL}`;
    }
    return 'https://pdf.in100tiva.com';
}