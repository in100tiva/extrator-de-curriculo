// api/queue-status.js
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

// Initialize Firebase Admin
if (!getApps().length) {
    try {
        const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
        initializeApp({ credential: cert(serviceAccount) });
    } catch (e) {
        console.error("Firebase Admin initialization error:", e);
        throw e;
    }
}
const db = getFirestore();

export default async function handler(request, response) {
    // Enable CORS
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    response.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    
    if (request.method === 'OPTIONS') {
        return response.status(200).end();
    }

    if (request.method !== 'POST') {
        return response.status(405).json({ error: 'Method not allowed' });
    }

    try {
        const { userId, jobIds } = request.body;
        
        if (!userId) {
            return response.status(400).json({ error: 'User ID is required' });
        }

        // Get status for specific jobs or all user jobs
        let query = db.collection('processing_queue')
            .where('userId', '==', userId);
        
        // Add time limit to avoid querying old jobs
        const oneDayAgo = new Date();
        oneDayAgo.setDate(oneDayAgo.getDate() - 1);
        query = query.where('createdAt', '>=', oneDayAgo);
        
        if (jobIds && Array.isArray(jobIds) && jobIds.length > 0) {
            // Firestore 'in' query is limited to 10 items
            const batchSize = 10;
            const results = [];
            
            for (let i = 0; i < jobIds.length; i += batchSize) {
                const batch = jobIds.slice(i, i + batchSize);
                const snapshot = await db.collection('processing_queue')
                    .where('userId', '==', userId)
                    .where('__name__', 'in', batch)
                    .get();
                
                snapshot.docs.forEach(doc => {
                    const data = doc.data();
                    results.push({
                        jobId: doc.id,
                        status: data.status,
                        fileName: data.fileName,
                        error: data.error,
                        createdAt: data.createdAt?.toDate(),
                        finishedAt: data.finishedAt?.toDate()
                    });
                });
            }
            
            return response.status(200).json({
                success: true,
                jobs: results,
                stats: calculateStats(results)
            });
        } else {
            // Get all recent jobs for user
            const snapshot = await query.limit(200).get();
            
            const jobs = snapshot.docs.map(doc => {
                const data = doc.data();
                return {
                    jobId: doc.id,
                    status: data.status,
                    fileName: data.fileName,
                    error: data.error,
                    createdAt: data.createdAt?.toDate(),
                    finishedAt: data.finishedAt?.toDate()
                };
            });
            
            return response.status(200).json({
                success: true,
                jobs: jobs,
                stats: calculateStats(jobs)
            });
        }
    } catch (error) {
        console.error('[QUEUE-STATUS] Error:', error);
        return response.status(500).json({ 
            error: 'Failed to get queue status',
            message: error.message 
        });
    }
}

function calculateStats(jobs) {
    const stats = {
        total: jobs.length,
        pending: 0,
        processing: 0,
        completed: 0,
        failed: 0
    };
    
    jobs.forEach(job => {
        if (job.status === 'pending') stats.pending++;
        else if (job.status === 'processing') stats.processing++;
        else if (job.status === 'completed') stats.completed++;
        else if (job.status === 'failed') stats.failed++;
    });
    
    return stats;
}