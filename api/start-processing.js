import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

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

    try {
        console.log(`[START] Iniciador Autocorretivo para o usuário ${userId}`);
        const queueRef = db.collection('processing_queue');

        // Lógica Autocorretiva Melhorada
        const stuckJobsSnapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'processing')
            .get();

        if (!stuckJobsSnapshot.empty) {
            console.log(`[START] ${stuckJobsSnapshot.size} job(s) travado(s) encontrado(s). Resetando...`);
            const batch = db.batch();
            const currentTime = new Date();
            
            stuckJobsSnapshot.docs.forEach(doc => {
                const jobData = doc.data();
                const retryCount = jobData.retryCount || 0;
                
                // Só reseta se não excedeu tentativas ou se está travado há muito tempo
                const startedAt = jobData.startedAt?.toDate();
                const isStuckTooLong = startedAt && (currentTime - startedAt) > 60000; // 1 minuto
                
                if (retryCount < 2 || isStuckTooLong) {
                    batch.update(doc.ref, { 
                        status: 'pending',
                        retryCount: retryCount + (isStuckTooLong ? 0 : 1)
                    });
                } else {
                    // Marca como failed se excedeu tentativas
                    batch.update(doc.ref, { 
                        status: 'failed',
                        error: 'Excedeu tentativas máximas'
                    });
                }
            });
            await batch.commit();
            console.log(`[START] Jobs travados processados.`);
        }

        // Processa múltiplos jobs em paralelo (limitado para evitar sobrecarga)
        const pendingSnapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt')
            .limit(3) // Máximo 3 jobs simultâneos na Vercel gratuita
            .get();

        if (pendingSnapshot.empty) {
            console.log(`[START] Fila para ${userId} está vazia.`);
            return response.status(200).send('Queue is empty, no action taken.');
        }

        const host = request.headers.host;
        const protocol = host.includes('localhost') ? 'http' : 'https';
        
        // Dispara jobs com delay escalonado para evitar sobrecarga
        const jobPromises = pendingSnapshot.docs.map((doc, index) => {
            return new Promise((resolve) => {
                setTimeout(async () => {
                    const jobId = doc.id;
                    console.log(`[START] Acionando job ${jobId} (${index + 1}/${pendingSnapshot.size})`);
                    
                    try {
                        // Disparo fire-and-forget para evitar timeouts em cascata
                        fetch(`${protocol}://${host}/api/process-job`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ jobId: jobId, userId: userId })
                        }).catch(err => {
                            console.error(`[START] Erro ao acionar job ${jobId}:`, err);
                        });
                        
                        resolve({ success: true, jobId });
                    } catch (error) {
                        console.error(`[START] Erro crítico com job ${jobId}:`, error);
                        resolve({ success: false, jobId, error: error.message });
                    }
                }, index * 500); // Delay escalonado: 0ms, 500ms, 1000ms
            });
        });

        // Não aguarda todos os jobs terminarem para evitar timeout
        console.log(`[START] ${pendingSnapshot.size} job(s) disparado(s) com delay escalonado`);
        
        // Resposta rápida
        response.status(202).send(`Processing initiated for ${pendingSnapshot.size} jobs.`);

        // Executa promises em background (não bloqueia resposta)
        Promise.allSettled(jobPromises).then(results => {
            const successful = results.filter(r => r.value?.success).length;
            const failed = results.filter(r => !r.value?.success).length;
            console.log(`[START] Resumo: ${successful} jobs iniciados, ${failed} falharam`);
        }).catch(error => {
            console.error('[START] Erro no processamento em background:', error);
        });

    } catch (error) {
        console.error(`[START] Erro ao iniciar o processamento para ${userId}:`, error);
        response.status(500).send('Failed to start processing queue.');
    }
}