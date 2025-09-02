import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

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

export default async function handler(request, response) {
    if (request.method !== 'POST') return response.status(405).send('Method Not Allowed');
    
    const { userId } = request.body;
    if (!userId) return response.status(400).send('User ID is required.');

    console.log(`[START-PROCESSING] === INICIANDO PARA USUÁRIO ${userId} ===`);

    try {
        // ETAPA 1: Limpeza e correção de jobs travados
        const cleanupResult = await cleanupStuckJobs(userId);
        console.log(`[START-PROCESSING] Limpeza: ${cleanupResult.reset} jobs resetados, ${cleanupResult.failed} marcados como failed`);

        // ETAPA 2: Encontrar o primeiro job pendente
        const firstJob = await findFirstPendingJob(userId);
        
        if (!firstJob) {
            console.log(`[START-PROCESSING] ✅ Nenhum job pendente encontrado para ${userId}`);
            return response.status(200).json({ 
                success: true, 
                message: 'No pending jobs found' 
            });
        }

        console.log(`[START-PROCESSING] 🎯 Primeiro job pendente: ${firstJob.id} (${firstJob.data.fileName})`);

        // ETAPA 3: Disparar o primeiro job
        const triggerResult = await triggerFirstJob(firstJob.id, userId);
        
        if (triggerResult.success) {
            console.log(`[START-PROCESSING] ✅ Processamento iniciado com sucesso`);
            return response.status(202).json({ 
                success: true, 
                message: `Processing started with job ${firstJob.id}`,
                jobId: firstJob.id 
            });
        } else {
            console.error(`[START-PROCESSING] ❌ Falha ao disparar primeiro job: ${triggerResult.error}`);
            return response.status(500).json({ 
                success: false, 
                error: 'Failed to trigger first job' 
            });
        }

    } catch (error) {
        console.error(`[START-PROCESSING] 💥 ERRO CRÍTICO:`, error);
        return response.status(500).json({ 
            success: false, 
            error: 'Critical error in start processing' 
        });
    }
}

/**
 * Limpa jobs que podem ter travado
 */
async function cleanupStuckJobs(userId) {
    const now = Timestamp.now();
    const fiveMinutesAgo = Timestamp.fromMillis(now.toMillis() - 5 * 60 * 1000);
    
    try {
        // Busca jobs em processamento há muito tempo
        const stuckJobsSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'processing')
            .get();

        if (stuckJobsSnapshot.empty) {
            return { reset: 0, failed: 0 };
        }

        console.log(`[CLEANUP] Encontrados ${stuckJobsSnapshot.size} jobs em processamento`);

        const batch = db.batch();
        let resetCount = 0;
        let failedCount = 0;

        stuckJobsSnapshot.docs.forEach(doc => {
            const jobData = doc.data();
            const startedAt = jobData.startedAt || jobData.claimedAt;
            
            if (startedAt && startedAt.toMillis() < fiveMinutesAgo.toMillis()) {
                // Job travado há mais de 5 minutos - marca como failed
                batch.update(doc.ref, {
                    status: 'failed',
                    finishedAt: now,
                    error: 'Job travado - timeout de 5 minutos',
                    cleanedUp: true
                });
                failedCount++;
                console.log(`[CLEANUP] Job ${doc.id} marcado como failed (travado há muito tempo)`);
            } else {
                // Job recente - reseta para pending
                batch.update(doc.ref, {
                    status: 'pending',
                    resetAt: now
                });
                resetCount++;
                console.log(`[CLEANUP] Job ${doc.id} resetado para pending`);
            }
        });

        if (resetCount > 0 || failedCount > 0) {
            await batch.commit();
            console.log(`[CLEANUP] ✅ Limpeza concluída: ${resetCount} resetados, ${failedCount} failed`);
        }

        return { reset: resetCount, failed: failedCount };

    } catch (error) {
        console.error('[CLEANUP] Erro na limpeza:', error);
        return { reset: 0, failed: 0 };
    }
}

/**
 * Encontra o primeiro job pendente
 */
async function findFirstPendingJob(userId) {
    try {
        const pendingSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt', 'asc')
            .limit(1)
            .get();

        if (pendingSnapshot.empty) {
            return null;
        }

        const doc = pendingSnapshot.docs[0];
        return {
            id: doc.id,
            data: doc.data()
        };

    } catch (error) {
        console.error('[FIND-FIRST] Erro ao buscar primeiro job:', error);
        return null;
    }
}

/**
 * Dispara o primeiro job da fila
 */
async function triggerFirstJob(jobId, userId) {
    try {
        const baseUrl = getBaseUrl();
        console.log(`[TRIGGER] Disparando job ${jobId} via ${baseUrl}/api/process-job`);

        const response = await fetch(`${baseUrl}/api/process-job`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'User-Agent': 'vercel-function'
            },
            body: JSON.stringify({ jobId, userId }),
            timeout: 8000 // 8 segundos de timeout
        });

        if (!response.ok) {
            const errorText = await response.text().catch(() => 'Erro desconhecido');
            throw new Error(`HTTP ${response.status}: ${errorText}`);
        }

        const result = await response.json().catch(() => ({ success: true }));
        console.log(`[TRIGGER] ✅ Job ${jobId} disparado:`, result);

        return { success: true, result };

    } catch (error) {
        console.error(`[TRIGGER] ❌ Erro ao disparar job ${jobId}:`, error);
        
        // Em caso de erro, tenta marcar o job como failed para não travar a fila
        try {
            await db.collection('processing_queue').doc(jobId).update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: `Falha ao disparar job: ${error.message}`,
                triggerFailed: true
            });
            console.log(`[TRIGGER] Job ${jobId} marcado como failed devido a erro no disparo`);
        } catch (updateError) {
            console.error(`[TRIGGER] Erro ao marcar job como failed:`, updateError);
        }

        return { success: false, error: error.message };
    }
}

/**
 * Obtém URL base da aplicação
 */
function getBaseUrl() {
    // URL de produção específica
    if (process.env.VERCEL_ENV === 'production') {
        return 'https://pdf.in100tiva.com';
    }
    
    // Em preview/desenvolvimento na Vercel
    if (process.env.VERCEL_URL) {
        return `https://${process.env.VERCEL_URL}`;
    }
    
    // Fallback para desenvolvimento local
    return process.env.NODE_ENV === 'development' 
        ? 'http://localhost:3000' 
        : 'https://pdf.in100tiva.com';
}