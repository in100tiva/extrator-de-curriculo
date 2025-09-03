// api/queue-status.js - API para verificar status da fila
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

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
    maxDuration: 5,
};

export default async function handler(request, response) {
    if (request.method !== 'GET') {
        return response.status(405).json({ error: 'Method Not Allowed' });
    }
    
    const { userId } = request.query;
    if (!userId) {
        return response.status(400).json({ error: 'User ID is required' });
    }

    try {
        // Consulta simples para obter todos os jobs do usuário
        const snapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .limit(500) // Limite para não sobrecarregar
            .get();

        if (snapshot.empty) {
            return response.status(200).json({
                total: 0,
                pending: 0,
                processing: 0,
                completed: 0,
                failed: 0,
                progress: 100
            });
        }

        // Conta status localmente (sem consultas múltiplas)
        const stats = {
            total: 0,
            pending: 0,
            processing: 0,
            completed: 0,
            failed: 0
        };

        snapshot.docs.forEach(doc => {
            const status = doc.data().status || 'pending';
            stats.total++;
            stats[status] = (stats[status] || 0) + 1;
        });

        // Calcula progresso
        const finished = stats.completed + stats.failed;
        const progress = stats.total > 0 ? Math.round((finished / stats.total) * 100) : 0;

        // Dispara próximo batch se há jobs pendentes e nenhum processando
        if (stats.pending > 0 && stats.processing === 0) {
            console.log(`[QUEUE-STATUS] Disparando batch para ${userId} - ${stats.pending} pendentes`);
            
            setTimeout(() => {
                fetch(`${getBaseUrl()}/api/process-batch`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ userId, batchSize: 5 })
                }).catch(err => console.error('[QUEUE-STATUS] Erro ao disparar batch:', err));
            }, 100);
        }

        return response.status(200).json({
            ...stats,
            progress,
            isComplete: progress >= 100,
            needsProcessing: stats.pending > 0 && stats.processing === 0
        });

    } catch (error) {
        console.error(`[QUEUE-STATUS] Erro para ${userId}:`, error);
        return response.status(200).json({
            error: 'Erro ao verificar status',
            total: 0,
            pending: 0,
            processing: 0,
            completed: 0,
            failed: 0,
            progress: 0
        });
    }
}

function getBaseUrl() {
    if (process.env.VERCEL_URL) {
        return `https://${process.env.VERCEL_URL}`;
    }
    return 'https://pdf.in100tiva.com';
}