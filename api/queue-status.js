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
    if (request.method !== 'GET' && request.method !== 'POST') {
        return response.status(405).send('Method Not Allowed');
    }
    
    const { userId } = request.method === 'GET' ? request.query : request.body;
    if (!userId) return response.status(400).send('User ID is required.');

    try {
        console.log(`[QUEUE-STATUS] Verificando status para ${userId}`);
        
        // Busca todos os jobs do usuário
        const userJobsSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .get();

        if (userJobsSnapshot.empty) {
            return response.status(200).json({
                success: true,
                total: 0,
                pending: 0,
                processing: 0,
                completed: 0,
                failed: 0,
                message: 'No jobs found'
            });
        }

        // Contabiliza os status
        const stats = {
            total: userJobsSnapshot.size,
            pending: 0,
            processing: 0,
            completed: 0,
            failed: 0,
            stuck: 0
        };

        const now = Timestamp.now();
        const tenMinutesAgo = Timestamp.fromMillis(now.toMillis() - 10 * 60 * 1000);
        const jobs = [];

        userJobsSnapshot.docs.forEach(doc => {
            const job = doc.data();
            const jobInfo = {
                id: doc.id,
                fileName: job.fileName,
                status: job.status,
                createdAt: job.createdAt?.toDate(),
                finishedAt: job.finishedAt?.toDate(),
                startedAt: job.startedAt?.toDate()
            };

            stats[job.status]++;

            // Identifica jobs travados
            if (job.status === 'processing' && job.startedAt && job.startedAt.toMillis() < tenMinutesAgo.toMillis()) {
                stats.stuck++;
                jobInfo.isStuck = true;
            }

            jobs.push(jobInfo);
        });

        // Calcula estatísticas adicionais
        const completionRate = stats.total > 0 ? ((stats.completed / stats.total) * 100).toFixed(1) : 0;
        const failureRate = stats.total > 0 ? ((stats.failed / stats.total) * 100).toFixed(1) : 0;
        
        // Identifica se precisa de intervenção
        const needsIntervention = stats.stuck > 0 || (stats.processing > 5) || (stats.pending > 0 && stats.processing === 0);

        // Calcula tempo médio de processamento para jobs completos
        let avgProcessingTime = 0;
        const completedJobs = jobs.filter(j => j.status === 'completed' && j.startedAt && j.finishedAt);
        if (completedJobs.length > 0) {
            const totalTime = completedJobs.reduce((sum, job) => {
                return sum + (new Date(job.finishedAt) - new Date(job.startedAt));
            }, 0);
            avgProcessingTime = Math.round(totalTime / completedJobs.length / 1000); // em segundos
        }

        const result = {
            success: true,
            userId: userId,
            timestamp: new Date().toISOString(),
            stats: stats,
            metrics: {
                completionRate: parseFloat(completionRate),
                failureRate: parseFloat(failureRate),
                avgProcessingTime: avgProcessingTime,
                needsIntervention: needsIntervention
            },
            jobs: jobs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt)), // Mais recentes primeiro
            recommendations: generateRecommendations(stats, needsIntervention)
        };

        console.log(`[QUEUE-STATUS] ✅ Status: ${stats.completed}/${stats.total} completos, ${stats.pending} pendentes, ${stats.stuck} travados`);
        
        return response.status(200).json(result);

    } catch (error) {
        console.error(`[QUEUE-STATUS] Erro:`, error);
        return response.status(500).json({
            success: false,
            error: 'Failed to get queue status',
            details: error.message
        });
    }
}

/**
 * Gera recomendações baseadas no status da fila
 */
function generateRecommendations(stats, needsIntervention) {
    const recommendations = [];

    if (stats.stuck > 0) {
        recommendations.push({
            type: 'warning',
            message: `${stats.stuck} job(s) travado(s) detectado(s)`,
            action: 'cleanup',
            description: 'Execute limpeza para resetar jobs travados'
        });
    }

    if (stats.pending > 0 && stats.processing === 0) {
        recommendations.push({
            type: 'action',
            message: `${stats.pending} job(s) pendente(s) sem processamento ativo`,
            action: 'restart',
            description: 'Execute start-processing para continuar a fila'
        });
    }

    if (stats.processing > 3) {
        recommendations.push({
            type: 'info',
            message: `${stats.processing} job(s) sendo processado(s) simultaneamente`,
            action: 'monitor',
            description: 'Monitore para evitar sobrecarga'
        });
    }

    if (stats.failed > stats.completed && stats.total > 5) {
        recommendations.push({
            type: 'warning',
            message: 'Taxa de falha alta detectada',
            action: 'investigate',
            description: 'Verifique logs para identificar problemas'
        });
    }

    if (stats.total > 100) {
        recommendations.push({
            type: 'maintenance',
            message: 'Muitos jobs acumulados',
            action: 'cleanup_old',
            description: 'Execute limpeza de jobs antigos para otimizar performance'
        });
    }

    if (recommendations.length === 0) {
        recommendations.push({
            type: 'success',
            message: 'Fila operando normalmente',
            action: 'none',
            description: 'Nenhuma ação necessária'
        });
    }

    return recommendations;
}