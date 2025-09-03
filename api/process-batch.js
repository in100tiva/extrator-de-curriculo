// api/process-batch.js
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp } from 'firebase-admin/firestore';

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
    // Set timeout warning
    const timeoutWarning = setTimeout(() => {
        console.log('[PROCESS-BATCH] ⚠️ Approaching timeout limit (8s)');
    }, 8000);

    try {
        if (request.method !== 'POST') {
            return response.status(405).json({ error: 'Method not allowed' });
        }

        const { userId, jobIds } = request.body;
        if (!userId || !jobIds || !Array.isArray(jobIds)) {
            return response.status(400).json({ error: 'Invalid request parameters' });
        }

        console.log(`[PROCESS-BATCH] Processing batch of ${jobIds.length} jobs for user ${userId}`);

        // Process jobs in parallel with concurrency limit
        const CONCURRENT_LIMIT = 3; // Process 3 at a time
        const results = [];
        
        for (let i = 0; i < jobIds.length; i += CONCURRENT_LIMIT) {
            const batch = jobIds.slice(i, i + CONCURRENT_LIMIT);
            const batchPromises = batch.map(jobId => processJob(jobId, userId));
            const batchResults = await Promise.allSettled(batchPromises);
            results.push(...batchResults);
            
            // Small delay between sub-batches to avoid rate limiting
            if (i + CONCURRENT_LIMIT < jobIds.length) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        const successful = results.filter(r => r.status === 'fulfilled').length;
        const failed = results.filter(r => r.status === 'rejected').length;

        console.log(`[PROCESS-BATCH] Completed: ${successful} success, ${failed} failed`);
        clearTimeout(timeoutWarning);

        return response.status(200).json({
            success: true,
            processed: successful,
            failed: failed,
            total: jobIds.length
        });

    } catch (error) {
        console.error('[PROCESS-BATCH] Error:', error);
        clearTimeout(timeoutWarning);
        return response.status(500).json({ 
            error: 'Batch processing error',
            message: error.message 
        });
    }
}

async function processJob(jobId, userId) {
    try {
        const jobRef = db.collection('processing_queue').doc(jobId);
        
        // Use transaction for atomic claim
        const result = await db.runTransaction(async (transaction) => {
            const jobDoc = await transaction.get(jobRef);
            
            if (!jobDoc.exists) {
                throw new Error('Job not found');
            }

            const jobData = jobDoc.data();
            
            // Verify ownership and status
            if (jobData.userId !== userId) {
                throw new Error('Unauthorized');
            }
            
            if (jobData.status !== 'pending') {
                return { skipped: true, status: jobData.status };
            }

            // Mark as processing
            transaction.update(jobRef, {
                status: 'processing',
                startedAt: Timestamp.now()
            });

            return { jobData, shouldProcess: true };
        });

        if (result.skipped) {
            console.log(`[PROCESS-JOB ${jobId}] Skipped (status: ${result.status})`);
            return { jobId, skipped: true };
        }

        if (!result.shouldProcess) {
            return { jobId, skipped: true };
        }

        // Extract data with Gemini API
        const extractionResult = await extractWithGemini(
            result.jobData.text,
            result.jobData.selectedFields,
            jobId
        );

        // Update with results
        await jobRef.update({
            status: extractionResult.success ? 'completed' : 'failed',
            finishedAt: Timestamp.now(),
            ...(extractionResult.success 
                ? { result: extractionResult.data }
                : { error: extractionResult.error })
        });

        console.log(`[PROCESS-JOB ${jobId}] ${extractionResult.success ? '✅ Success' : '❌ Failed'}`);
        return { 
            jobId, 
            success: extractionResult.success,
            data: extractionResult.data 
        };

    } catch (error) {
        console.error(`[PROCESS-JOB ${jobId}] Error:`, error.message);
        
        // Try to mark as failed
        try {
            await db.collection('processing_queue').doc(jobId).update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: error.message
            });
        } catch (updateError) {
            console.error(`[PROCESS-JOB ${jobId}] Failed to update status:`, updateError);
        }
        
        throw error;
    }
}

async function extractWithGemini(text, selectedFields, jobId) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        return fallbackExtraction(text, selectedFields);
    }

    try {
        const currentYear = new Date().getFullYear();
        const truncatedText = text.substring(0, 2000); // Limit text size
        
        // Build dynamic schema based on selected fields
        const properties = { nome: { type: "STRING" } };
        const required = ["nome"];
        
        if (selectedFields.includes('idade')) {
            properties.idade = { type: "NUMBER" };
            required.push('idade');
        }
        if (selectedFields.includes('email')) {
            properties.email = { type: "STRING" };
            required.push('email');
        }
        if (selectedFields.includes('contatos')) {
            properties.contatos = { type: "ARRAY", items: { type: "STRING" } };
            required.push('contatos');
        }

        const prompt = `Extract resume data from the following text. 
Current year is ${currentYear}.
For age: Look for "X anos" or calculate from birth date.
For contacts: Format as (DD) 9XXXX-XXXX.
Return ONLY valid JSON.

Text: ${truncatedText}`;

        const payload = {
            contents: [{ parts: [{ text: prompt }] }],
            generationConfig: {
                responseMimeType: "application/json",
                responseSchema: {
                    type: "OBJECT",
                    properties,
                    required
                },
                temperature: 0,
                maxOutputTokens: 256
            }
        };

        // Tight timeout for Gemini API
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);

        const response = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${apiKey}`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
                signal: controller.signal
            }
        );

        clearTimeout(timeoutId);

        if (!response.ok) {
            throw new Error(`Gemini API error: ${response.status}`);
        }

        const result = await response.json();
        const content = result.candidates?.[0]?.content?.parts?.[0]?.text;
        
        if (!content) {
            throw new Error('No content from Gemini');
        }

        const parsedData = JSON.parse(content);
        
        // Validate and clean data
        if (parsedData.idade && (parsedData.idade < 14 || parsedData.idade > 100)) {
            parsedData.idade = 0;
        }
        
        return { success: true, data: parsedData };

    } catch (error) {
        console.log(`[GEMINI ${jobId}] Error, using fallback:`, error.message);
        return fallbackExtraction(text, selectedFields);
    }
}

function fallbackExtraction(text, selectedFields) {
    const result = { nome: 'Nome não identificado' };
    
    try {
        // Extract name - simple pattern matching
        const namePatterns = [
            /^([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-zàáâãäéêëíîïóôõöúûüç]+(?: [A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-zàáâãäéêëíîïóôõöúûüç]+)+)/m,
            /Nome:?\s*([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][^\n\r]{5,50})/i
        ];
        
        for (const pattern of namePatterns) {
            const match = text.match(pattern);
            if (match?.[1]) {
                result.nome = match[1].trim();
                break;
            }
        }
        
        // Extract age if needed
        if (selectedFields.includes('idade')) {
            const ageMatch = text.match(/(\d{1,2})\s*anos/i);
            if (ageMatch) {
                const age = parseInt(ageMatch[1]);
                result.idade = (age >= 14 && age <= 100) ? age : 0;
            } else {
                result.idade = 0;
            }
        }
        
        // Extract email if needed
        if (selectedFields.includes('email')) {
            const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            result.email = emailMatch ? emailMatch[1].toLowerCase() : '';
        }
        
        // Extract contacts if needed
        if (selectedFields.includes('contatos')) {
            const phoneMatches = text.matchAll(/\(?\d{2}\)?\s*9?\d{4,5}[\s-]?\d{4}/g);
            const contacts = [];
            
            for (const match of phoneMatches) {
                const cleaned = match[0].replace(/\D/g, '');
                if (cleaned.length >= 10 && cleaned.length <= 11) {
                    const formatted = cleaned.length === 11
                        ? `(${cleaned.slice(0, 2)}) ${cleaned.slice(2, 7)}-${cleaned.slice(7)}`
                        : `(${cleaned.slice(0, 2)}) ${cleaned.slice(2, 6)}-${cleaned.slice(6)}`;
                    contacts.push(formatted);
                    if (contacts.length >= 2) break; // Limit to 2 contacts
                }
            }
            
            result.contatos = contacts;
        }
        
        return { success: true, data: result };
        
    } catch (error) {
        console.error('[FALLBACK] Extraction error:', error);
        return { 
            success: true, // Still mark as success to not block the queue
            data: { nome: 'Erro na extração', ...result }
        };
    }
}