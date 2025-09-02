import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

// Bloco de inicialização robusto
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

        // Lógica Autocorretiva: Reseta jobs que possam ter travado
        const stuckJobsSnapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'processing')
            .get();

        if (!stuckJobsSnapshot.empty) {
            console.log(`[START] ${stuckJobsSnapshot.size} job(s) travado(s) encontrado(s). Resetando para 'pending'...`);
            const batch = db.batch();
            stuckJobsSnapshot.docs.forEach(doc => {
                batch.update(doc.ref, { status: 'pending' });
            });
            await batch.commit();
            console.log(`[START] Jobs travados resetados.`);
        }

        // Remove jobs já concluídos ou falhos para evitar acúmulo na fila
        for (const status of ['completed', 'failed']) {
            const doneSnapshot = await queueRef
                .where('userId', '==', userId)
                .where('status', '==', status)
                .get();
            if (!doneSnapshot.empty) {
                console.log(`[START] ${doneSnapshot.size} job(s) com status '${status}' removido(s).`);
                const batch = db.batch();
                doneSnapshot.docs.forEach(doc => batch.delete(doc.ref));
                await batch.commit();
            }
        }

        // Procede para encontrar o primeiro job pendente
        const snapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt')
            .limit(1)
            .get();

        if (snapshot.empty) {
            console.log(`[START] Fila para ${userId} está vazia. Nada a fazer.`);
            return response.status(200).send('Queue is empty, no action taken.');
        }

        const firstJobId = snapshot.docs[0].id;
        console.log(`[START] Primeiro job pendente encontrado: ${firstJobId}. Acionando o processador...`);

        // Dispara o trabalhador e aguarda a confirmação do envio.
        const host = request.headers.host;
        const protocol = host.includes('localhost') ? 'http' : 'https';
        try {
            const res = await fetch(`${protocol}://${host}/api/process-job`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ jobId: firstJobId, userId })
            });
            const resText = await res.text().catch(() => '');
            if (!res.ok) {
                console.error(`[START] process-job retornou ${res.status} para ${firstJobId}: ${resText}`);
            }
        } catch (err) {
            console.error(`[START] Erro ao acionar o process-job para ${firstJobId}:`, err);
        }

        response.status(202).send('Processing has been initiated.');

    } catch (error) {
        console.error(`[START] Erro ao iniciar o processamento para ${userId}:`, error);
        response.status(500).send('Failed to start processing queue.');
    }
}

