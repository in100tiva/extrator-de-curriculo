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
        // ETAPA 1: Limpeza de jobs antigos (mais de 24 horas)
        const cleanupOldResult = await cleanupOldJobs(userId);
        if (cleanupOldResult.deleted > 0) {
            console.log(`[START-PROCESSING] Removidos ${cleanupOldResult.deleted} jobs antigos`);
        }

        // ETAPA 2: Limpeza e correção de jobs travados
        const cleanupResult = await cleanupStuckJobs(userId);
        console.log(`[START-PROCESSING] Limpeza: ${cleanupResult.reset} jobs resetados, ${cleanupResult.failed} marcados como failed`);

        // ETAPA 3: Encontrar jobs pendentes (múltiplos para processamento paralelo limitado)
        const pendingJobs = await findPendingJobs(userId, 3); // Máximo 3 simultâneos
        
        if (!pendingJobs || pendingJobs.length === 0) {
            console.log(`[START-PROCESSING] ✅ Nenhum job pendente encontrado para ${userId}`);
            return response.status(200).json({ 
                success: true, 
                message: 'No pending jobs found' 
            });
        }

        console.log(`[START-PROCESSING] 🎯 ${pendingJobs.length} job(s) pendente(s) encontrado(s)`);

        // ETAPA 4: Disparar jobs com delay escalonado
        const triggerResults = await triggerMultipleJobs(pendingJobs, userId);
        
        const successfulJobs = triggerResults.filter(r => r.success).length;
        const failedJobs = triggerResults.filter(r => !r.success).length;

        if (successfulJobs > 0) {
            console.log(`[START-PROCESSING] ✅ ${successfulJobs} job(s) iniciado(s) com sucesso, ${failedJobs} falharam`);
            return response.status(202).json({ 
                success: true, 
                message: `Processing started for ${successfulJobs} jobs`,
                successful: successfulJobs,
                failed: failedJobs
            });
        } else {
            console.error(`[START-PROCESSING] ❌ Falha ao disparar todos os jobs`);
            return response.status(500).json({ 
                success: false, 
                error: 'Failed to trigger any jobs' 
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
 * Remove jobs antigos (completed/failed) para evitar acúmulo
 */
async function cleanupOldJobs(userId) {
    try {
        // CORREÇÃO: Query simples sem índice composto
        // Busca jobs do usuário primeiro, depois filtra por data
        const userJobsSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .limit(100) // Limita para não sobrecarregar
            .get();

        if (userJobsSnapshot.empty) {
            return { deleted: 0 };
        }

        const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
        const batch = db.batch();
        let deleteCount = 0;

        userJobsSnapshot.docs.forEach(doc => {
            const jobData = doc.data();
            
            // Verifica se é antigo e finalizado
            if ((jobData.status === 'completed' || jobData.status === 'failed') && 
                jobData.finishedAt && 
                jobData.finishedAt.toMillis() < oneDayAgo) {
                
                batch.delete(doc.ref);
                deleteCount++;
                console.log(`[CLEANUP-OLD] Agendando remoção: ${doc.id} (${jobData.status})`);
            }
        });

        if (deleteCount > 0) {
            await batch.commit();
            console.log(`[CLEANUP-OLD] ✅ ${deleteCount} jobs antigos removidos`);
        }

        return { deleted: deleteCount };

    } catch (error) {
        console.error('[CLEANUP-OLD] Erro na limpeza de jobs antigos:', error);
        return { deleted: 0 };
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
 * Encontra múltiplos jobs pendentes (para processamento paralelo limitado)
 */
async function findPendingJobs(userId, limit = 3) {
    try {
        const pendingSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt', 'asc')
            .limit(limit)
            .get();

        if (pendingSnapshot.empty) {
            return [];
        }

        return pendingSnapshot.docs.map(doc => ({
            id: doc.id,
            data: doc.data()
        }));

    } catch (error) {
        console.error('[FIND-PENDING] Erro ao buscar jobs pendentes:', error);
        return [];
    }
}

/**
 * Dispara múltiplos jobs de forma assíncrona (fire-and-forget)
 */
async function triggerMultipleJobsAsync(jobs, userId) {
    console.log(`[TRIGGER-ASYNC] Disparando ${jobs.length} job(s) em background...`);
    
    const baseUrl = getBaseUrl();
    const results = [];

    // Dispara TODOS os jobs imediatamente (fire-and-forget)
    for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i];
        console.log(`[TRIGGER-ASYNC] Disparando job ${i + 1}/${jobs.length}: ${job.id}`);
        
        // Fire-and-forget com delay escalonado
        setTimeout(async () => {
            try {
                const response = await fetch(`${baseUrl}/api/process-single`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ jobId: job.id, userId }),
                });
                
                if (response.ok) {
                    console.log(`[TRIGGER-ASYNC] ✅ Job ${job.id} processado em background`);
                } else {
                    console.error(`[TRIGGER-ASYNC] ❌ Job ${job.id} falhou: HTTP ${response.status}`);
                }
            } catch (error) {
                console.error(`[TRIGGER-ASYNC] ❌ Erro no job ${job.id}:`, error.message);
                
                // Marca como failed para não travar
                try {
                    await db.collection('processing_queue').doc(job.id).update({
                        status: 'failed',
                        finishedAt: Timestamp.now(),
                        error: `Disparo assíncrono falhou: ${error.message}`
                    });
                } catch (updateError) {
                    console.error(`[TRIGGER-ASYNC] Erro ao marcar como failed:`, updateError);
                }
            }
        }, i * 3000); // 3 segundos entre cada job (mais tempo para evitar sobrecarga)
        
        // Marca como "disparado" para retorno imediato
        results.push({ success: true, jobId: job.id });
    }
    
    return results;
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