// api/cleanup-cron.js
// This endpoint can be called periodically to clean up old jobs
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

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
    // Optional: Add security token check
    const authToken = request.headers['x-cleanup-token'];
    if (authToken !== process.env.CLEANUP_TOKEN && process.env.CLEANUP_TOKEN) {
        return response.status(401).json({ error: 'Unauthorized' });
    }

    try {
        console.log('[CLEANUP-CRON] Starting cleanup job...');
        
        // Clean up jobs older than 24 hours
        const cutoffTime = Timestamp.fromMillis(Date.now() - 24 * 60 * 60 * 1000);
        
        // Query completed and failed jobs older than cutoff
        const oldJobsSnapshot = await db.collection('processing_queue')
            .where('finishedAt', '<=', cutoffTime)
            .limit(500) // Process in batches to avoid timeout
            .get();
        
        if (oldJobsSnapshot.empty) {
            console.log('[CLEANUP-CRON] No old jobs to clean');
            return response.status(200).json({ 
                success: true, 
                cleaned: 0,
                message: 'No old jobs found' 
            });
        }
        
        // Delete in batches of 100 (Firestore limit)
        const batches = [];
        let batch = db.batch();
        let operationCount = 0;
        let totalDeleted = 0;
        
        oldJobsSnapshot.docs.forEach(doc => {
            const data = doc.data();
            // Only delete completed or failed jobs
            if (data.status === 'completed' || data.status === 'failed') {
                batch.delete(doc.ref);
                operationCount++;
                totalDeleted++;
                
                // Create new batch every 100 operations
                if (operationCount === 100) {
                    batches.push(batch);
                    batch = db.batch();
                    operationCount = 0;
                }
            }
        });
        
        // Add remaining batch
        if (operationCount > 0) {
            batches.push(batch);
        }
        
        // Execute all batches
        await Promise.all(batches.map(b => b.commit()));
        
        console.log(`[CLEANUP-CRON] Cleaned ${totalDeleted} old jobs`);
        
        // Also clean up stuck processing jobs (older than 10 minutes)
        const stuckCutoff = Timestamp.fromMillis(Date.now() - 10 * 60 * 1000);
        const stuckJobsSnapshot = await db.collection('processing_queue')
            .where('status', '==', 'processing')
            .where('startedAt', '<=', stuckCutoff)
            .limit(100)
            .get();
        
        if (!stuckJobsSnapshot.empty) {
            const stuckBatch = db.batch();
            let stuckCount = 0;
            
            stuckJobsSnapshot.docs.forEach(doc => {
                stuckBatch.update(doc.ref, {
                    status: 'failed',
                    finishedAt: Timestamp.now(),
                    error: 'Job timeout - stuck in processing'
                });
                stuckCount++;
            });
            
            await stuckBatch.commit();
            console.log(`[CLEANUP-CRON] Reset ${stuckCount} stuck jobs`);
            
            return response.status(200).json({
                success: true,
                cleaned: totalDeleted,
                resetStuck: stuckCount,
                message: `Cleaned ${totalDeleted} old jobs and reset ${stuckCount} stuck jobs`
            });
        }
        
        return response.status(200).json({
            success: true,
            cleaned: totalDeleted,
            message: `Successfully cleaned ${totalDeleted} old jobs`
        });
        
    } catch (error) {
        console.error('[CLEANUP-CRON] Error:', error);
        return response.status(500).json({
            success: false,
            error: 'Cleanup failed',
            message: error.message
        });
    }
}