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
    
    const { userId, olderThanHours = 1 } = request.body;
    if (!userId) return response.status(400).send('User ID is required.');

    try {
        console.log(`[CLEANUP-API] Iniciando limpeza para ${userId}, mais de ${olderThanHours} horas`);
        
        const cutoffTime = Timestamp.fromMillis(Date.now() - olderThanHours * 60 * 60 * 1000);
        
        // Busca jobs finalizados antigos
        const oldJobsSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('finishedAt', '<=', cutoffTime)
            .get();

        if (oldJobsSnapshot.empty) {
            console.log(`[CLEANUP-API] Nenhum job antigo encontrado`);
            return response.status(200).json({ 
                success: true, 
                deleted: 0, 
                message: 'No old jobs found to clean' 
            });
        }

        console.log(`[CLEANUP-API] Encontrados ${oldJobsSnapshot.size} jobs para limpeza`);

        // Remove jobs em lotes
        const batch = db.batch();
        let deletedCount = 0;
        const deletedJobs = [];

        oldJobsSnapshot.docs.forEach(doc => {
            const jobData = doc.data();
            
            // Só remove completed e failed
            if (jobData.status === 'completed' || jobData.status === 'failed') {
                batch.delete(doc.ref);
                deletedCount++;
                deletedJobs.push({
                    id: doc.id,
                    fileName: jobData.fileName,
                    status: jobData.status,
                    finishedAt: jobData.finishedAt?.toDate()
                });
            }
        });

        if (deletedCount > 0) {
            await batch.commit();
            console.log(`[CLEANUP-API] ✅ ${deletedCount} jobs removidos com sucesso`);
        }

        return response.status(200).json({
            success: true,
            deleted: deletedCount,
            jobs: deletedJobs,
            message: `Successfully cleaned ${deletedCount} old jobs`
        });

    } catch (error) {
        console.error(`[CLEANUP-API] Erro na limpeza:`, error);
        return response.status(500).json({
            success: false,
            error: 'Failed to cleanup jobs',
            details: error.message
        });
    }
}