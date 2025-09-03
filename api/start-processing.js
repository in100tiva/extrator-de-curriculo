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

        // ETAPA 3: Encontrar jobs pendentes (processamento sequencial para Vercel gratuita)
        const pendingJobs = await findPendingJobs(userId, 5); // Máximo 5 para começar
        
        if (!pendingJobs || pendingJobs.length === 0) {
            console.log(`[START-PROCESSING] ✅ Nenhum job pendente encontrado para ${userId}`);
            return response.status(200).json({ 
                success: true, 
                message: 'No pending jobs found' 
            });
        }

        console.log(`[START-PROCESSING] 🎯 ${pendingJobs.length} job(s) pendente(s) encontrado(s)`);

        // ETAPA 4: Disparar processamento otimizado para Vercel gratuita
        const triggerResult = await triggerBatchProcessing(pendingJobs, userId);
        
        if (triggerResult.success) {
            console.log(`[START-PROCESSING] ✅ Processamento iniciado para ${pendingJobs.length} jobs`);
            return response.status(202).json({ 
                success: true, 
                message: `Batch processing started for ${pendingJobs.length} jobs`,
                jobCount: pendingJobs.length
            });
        } else {
            console.error(`[START-PROCESSING] ❌ Falha ao iniciar processamento em lote`);
            return response.status(500).json({ 
                success: false, 
                error: 'Failed to start batch processing' 
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
        const userJobsSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .limit(100)
            .get();

        if (userJobsSnapshot.empty) {
            return { deleted: 0 };
        }

        const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
        const batch = db.batch();
        let deleteCount = 0;

        userJobsSnapshot.docs.forEach(doc => {
            const jobData = doc.data();
            
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
    const tenMinutesAgo = Timestamp.fromMillis(now.toMillis() - 10 * 60 * 1000); // 10 minutos
    
    try {
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
            
            if (startedAt && startedAt.toMillis() < tenMinutesAgo.toMillis()) {
                // Job travado há mais de 10 minutos - marca como failed
                batch.update(doc.ref, {
                    status: 'failed',
                    finishedAt: now,
                    error: 'Job travado - timeout de 10 minutos',
                    cleanedUp: true
                });
                failedCount++;
                console.log(`[CLEANUP] Job ${doc.id} marcado como failed (travado)`);
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
 * Encontra jobs pendentes para processamento
 */
async function findPendingJobs(userId, limit = 5) {
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
 * Dispara processamento em lote otimizado para Vercel gratuita
 * Usa estratégia de processamento sequencial com delays
 */
async function triggerBatchProcessing(jobs, userId) {
    try {
        console.log(`[BATCH] Iniciando processamento de ${jobs.length} jobs...`);
        
        const baseUrl = getBaseUrl();
        
        // Processa primeiro job imediatamente
        if (jobs.length > 0) {
            const firstJob = jobs[0];
            console.log(`[BATCH] Disparando primeiro job: ${firstJob.id}`);
            
            // Fire-and-forget para o primeiro job
            fetch(`${baseUrl}/api/process-single`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ jobId: firstJob.id, userId }),
            }).catch(error => {
                console.error(`[BATCH] Erro no primeiro job:`, error);
            });
        }
        
        // Agenda os demais jobs com delay escalonado
        for (let i = 1; i < jobs.length; i++) {
            const job = jobs[i];
            const delay = i * 8000; // 8 segundos entre cada job (mais conservador)
            
            setTimeout(async () => {
                try {
                    console.log(`[BATCH] Disparando job ${i + 1}/${jobs.length}: ${job.id}`);
                    
                    const response = await fetch(`${baseUrl}/api/process-single`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ jobId: job.id, userId }),
                    });
                    
                    if (response.ok) {
                        console.log(`[BATCH] ✅ Job ${job.id} processado`);
                        
                        // Após cada job, verifica se há mais jobs pendentes
                        setTimeout(() => checkAndProcessNext(userId), 2000);
                    } else {
                        console.error(`[BATCH] ❌ Job ${job.id} falhou: HTTP ${response.status}`);
                        await markJobAsFailed(job.id, `HTTP ${response.status}`);
                    }
                    
                } catch (error) {
                    console.error(`[BATCH] ❌ Erro no job ${job.id}:`, error.message);
                    await markJobAsFailed(job.id, error.message);
                }
            }, delay);
        }
        
        return { success: true };
        
    } catch (error) {
        console.error('[BATCH] Erro no processamento em lote:', error);
        return { success: false };
    }
}

/**
 * Verifica e processa próximos jobs automaticamente
 */
async function checkAndProcessNext(userId) {
    try {
        // Verifica se ainda há jobs pendentes
        const pendingJobs = await findPendingJobs(userId, 3);
        
        if (pendingJobs.length > 0) {
            console.log(`[CHECK-NEXT] Encontrados ${pendingJobs.length} jobs pendentes adicionais`);
            
            // Dispara mais uma rodada de processamento
            const baseUrl = getBaseUrl();
            fetch(`${baseUrl}/api/start-processing`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ userId }),
            }).catch(error => {
                console.error(`[CHECK-NEXT] Erro ao disparar próxima rodada:`, error);
            });
        } else {
            console.log(`[CHECK-NEXT] ✅ Todos os jobs processados para ${userId}`);
        }
        
    } catch (error) {
        console.error('[CHECK-NEXT] Erro ao verificar próximos jobs:', error);
    }
}

/**
 * Marca job como falha quando não consegue processar
 */
async function markJobAsFailed(jobId, errorMessage) {
    try {
        await db.collection('processing_queue').doc(jobId).update({
            status: 'failed',
            finishedAt: Timestamp.now(),
            error: `Falha no disparo: ${errorMessage}`
        });
        console.log(`[MARK-FAILED] Job ${jobId} marcado como failed`);
    } catch (updateError) {
        console.error(`[MARK-FAILED] Erro ao marcar job ${jobId} como failed:`, updateError);
    }
}

/**
 * Obtém URL base da aplicação
 */
function getBaseUrl() {
    if (process.env.VERCEL_ENV === 'production') {
        return 'https://pdf.in100tiva.com';
    }
    
    if (process.env.VERCEL_URL) {
        return `https://${process.env.VERCEL_URL}`;
    }
    
    return process.env.NODE_ENV === 'development' 
        ? 'http://localhost:3000' 
        : 'https://pdf.in100tiva.com';
}