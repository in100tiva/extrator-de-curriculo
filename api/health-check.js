import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

// Bloco de inicialização robusto
if (!getApps().length) {
    try {
        const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
        initializeApp({ credential: cert(serviceAccount) });
    } catch (e) {
        console.error("ERRO CRÍTICO: Falha na inicialização do Firebase Admin em health-check.", e);
        // Não lança erro para não parar o cron, apenas loga
    }
}
const db = getFirestore();

export default async function handler(request, response) {
    // Medida de segurança: apenas a Vercel pode chamar esta função
    if (request.headers['x-vercel-cron-secret'] !== process.env.VERCEL_CRON_SECRET) {
        return response.status(401).send('Unauthorized');
    }

    try {
        console.log('[HEALTH-CHECK] Iniciando verificação de jobs travados...');
        const queueRef = db.collection('processing_queue');
        
        // Procura por jobs que estão "em processamento" por mais de 90 segundos
        const ninetySecondsAgo = Timestamp.fromMillis(Date.now() - 90000);
        const stuckJobsSnapshot = await queueRef
            .where('status', '==', 'processing')
            .where('startedAt', '<', ninetySecondsAgo)
            .get();

        if (stuckJobsSnapshot.empty) {
            console.log('[HEALTH-CHECK] Nenhum job travado encontrado.');
            return response.status(200).send('No stuck jobs found.');
        }

        console.log(`[HEALTH-CHECK] ${stuckJobsSnapshot.size} job(s) travado(s) encontrado(s). Reiniciando...`);

        for (const jobDoc of stuckJobsSnapshot.docs) {
            const jobId = jobDoc.id;
            const jobData = jobDoc.data();
            const host = request.headers.host;
            const protocol = host.includes('localhost') ? 'http' : 'https';

            console.log(`[HEALTH-CHECK] Marcando job ${jobId} como pendente novamente.`);
            await jobDoc.ref.update({ status: 'pending' });

            console.log(`[HEALTH-CHECK] Reacionando o processamento para o job ${jobId}.`);
            fetch(`${protocol}://${host}/api/process-job`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ jobId: jobId, userId: jobData.userId })
            }).catch(err => console.error(`[HEALTH-CHECK] Erro ao reacionar o job ${jobId}:`, err));
            
            // Pausa para não sobrecarregar
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        response.status(200).send(`Restarted ${stuckJobsSnapshot.size} stuck job(s).`);

    } catch (error) {
        console.error('[HEALTH-CHECK] Erro durante a verificação:', error);
        response.status(500).send('Internal Server Error');
    }
}
