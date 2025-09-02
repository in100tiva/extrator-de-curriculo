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
        console.log(`[START] Iniciando a primeira busca na fila para o usuário ${userId}`);
        const queueRef = db.collection('processing_queue');
        const snapshot = await queueRef
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt')
            .limit(1)
            .get();

        if (snapshot.empty) {
            console.log(`[START] Fila para ${userId} já está vazia. Nada a fazer.`);
            return response.status(200).send('Queue is empty, no action taken.');
        }

        const firstJobId = snapshot.docs[0].id;
        console.log(`[START] Primeiro job encontrado: ${firstJobId}. Acionando o processador...`);

        const host = request.headers.host;
        const protocol = host.includes('localhost') ? 'http' : 'https';
        fetch(`${protocol}://${host}/api/process-job`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ jobId: firstJobId, userId: userId })
        }).catch(err => console.error(`[START] Erro ao acionar o process-job para ${firstJobId}:`, err));

        response.status(202).send('Processing has been initiated.');

    } catch (error) {
        console.error(`[START] Erro ao iniciar o processamento para ${userId}:`, error);
        response.status(500).send('Failed to start processing queue.');
    }
}

