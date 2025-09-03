// api/start-processing.js - Versão otimizada
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

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
    maxDuration: 10,
};

export default async function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).json({ error: 'Method Not Allowed' });
    }
    
    const { userId } = request.body;
    if (!userId) {
        return response.status(400).json({ error: 'User ID is required' });
    }

    console.log(`[START-PROCESSING] Iniciando para ${userId}`);

    try {
        // 1. Limpeza básica (sem consultas complexas)
        await basicCleanup(userId);

        // 2. Verificar se há jobs pendentes
        const hasPendingJobs = await checkPendingJobs(userId);
        
        if (!hasPendingJobs) {
            return response.status(200).json({ 
                success: true, 
                message: 'Nenhum job pendente encontrado' 
            });
        }

        // 3. Disparar processamento em lote (fire-and-forget)
        const baseUrl = getBaseUrl();
        setTimeout(() => {
            fetch(`${baseUrl}/api/process-batch`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ userId, batchSize: 5 })
            }).catch(err => console.error('[START] Erro ao disparar batch:', err));
        }, 500);

        return response.status(202).json({ 
            success: true, 
            message: 'Processamento em lote iniciado'
        });

    } catch (error) {
        console.error(`[START-PROCESSING] Erro:`, error);
        return response.status(200).json({ 
            success: false, 
            error: 'Erro ao iniciar processamento'
        });
    }
}

// Limpeza básica sem consultas complexas
async function basicCleanup(userId) {
    try {
        const fiveMinutesAgo = Timestamp.fromMillis(Date.now() - 5 * 60 * 1000);
        
        // Busca apenas jobs em processamento (consulta simples)
        const stuckSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'processing')
            .limit(20)
            .get();

        if (stuckSnapshot.empty) return;

        const batch = db.batch();
        let resetCount = 0;

        stuckSnapshot.docs.forEach(doc => {
            const jobData = doc.data();
            const startedAt = jobData.startedAt;
            
            // Se job está há mais de 5 minutos processando, reseta
            if (startedAt && startedAt.toMillis() < fiveMinutesAgo.toMillis()) {
                batch.update(doc.ref, {
                    status: 'pending',
                    resetAt: Timestamp.now()
                });
                resetCount++;
            }
        });

        if (resetCount > 0) {
            await batch.commit();
            console.log(`[CLEANUP] ${resetCount} jobs resetados`);
        }
        
    } catch (error) {
        console.error('[CLEANUP] Erro:', error);
    }
}

// Verifica jobs pendentes
async function checkPendingJobs(userId) {
    try {
        const snapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .limit(1)
            .get();
        
        return !snapshot.empty;
    } catch (error) {
        console.error('[CHECK-PENDING] Erro:', error);
        return false;
    }
}

// URL base
function getBaseUrl() {
    if (process.env.VERCEL_URL) {
        return `https://${process.env.VERCEL_URL}`;
    }
    return 'https://pdf.in100tiva.com';
}