/**
 * Endpoint simples para processar um único job sem chamar próximo
 * Usado para burlar o timeout da Vercel
 */

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

    const { jobId, userId } = request.body;
    if (!jobId || !userId) {
        return response.status(400).send('Job ID and User ID are required.');
    }

    console.log(`[PROCESS-SINGLE ${jobId}] Processando job único...`);

    try {
        // Reclama o job
        const jobResult = await claimJob(jobId, userId);
        if (!jobResult.success) {
            console.log(`[PROCESS-SINGLE ${jobId}] ${jobResult.message}`);
            return response.status(200).json({ success: false, message: jobResult.message });
        }

        // Processa
        const jobData = jobResult.data;
        console.log(`[PROCESS-SINGLE ${jobId}] Processando: ${jobData.fileName}`);

        let apiResult = await processWithGemini(jobData.text, jobData.selectedFields, jobId);
        
        // Fallback se necessário
        if (!apiResult.success && jobData.text) {
            console.log(`[PROCESS-SINGLE ${jobId}] Usando fallback...`);
            apiResult = extractWithFallback(jobData.text, jobData.selectedFields, jobId);
        }

        // Salva resultado
        const jobRef = db.collection('processing_queue').doc(jobId);
        if (apiResult.success) {
            await jobRef.update({
                status: 'completed',
                finishedAt: Timestamp.now(),
                result: apiResult.data
            });
            console.log(`[PROCESS-SINGLE ${jobId}] ✅ Concluído`);
        } else {
            await jobRef.update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: apiResult.error
            });
            console.log(`[PROCESS-SINGLE ${jobId}] ❌ Falhou: ${apiResult.error}`);
        }

        return response.status(200).json({ 
            success: apiResult.success, 
            jobId: jobId 
        });

    } catch (error) {
        console.error(`[PROCESS-SINGLE ${jobId}] Erro:`, error);
        
        try {
            await db.collection('processing_queue').doc(jobId).update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: error.message
            });
        } catch (updateError) {
            console.error(`[PROCESS-SINGLE ${jobId}] Erro ao marcar falha:`, updateError);
        }

        return response.status(200).json({ success: false, error: error.message });
    }
}

// Reutiliza as funções do process-job.js
async function claimJob(jobId, userId) {
    try {
        const jobRef = db.collection('processing_queue').doc(jobId);
        
        return await db.runTransaction(async (transaction) => {
            const jobDoc = await transaction.get(jobRef);
            
            if (!jobDoc.exists) {
                return { success: false, message: 'Job não encontrado' };
            }

            const jobData = jobDoc.data();
            
            if (jobData.userId !== userId) {
                return { success: false, message: 'Job não pertence ao usuário' };
            }

            if (jobData.status !== 'pending') {
                return { success: false, message: `Job já está em status: ${jobData.status}` };
            }

            transaction.update(jobRef, {
                status: 'processing',
                startedAt: Timestamp.now()
            });

            return { success: true, data: jobData };
        });

    } catch (error) {
        console.error(`[CLAIM-SINGLE ${jobId}] Erro:`, error);
        return { success: false, message: error.message };
    }
}

async function processWithGemini(text, selectedFields, jobId) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        return { success: false, error: 'GEMINI_API_KEY não configurada' };
    }

    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    
    const truncatedText = text.length > 2500 ? text.substring(0, 2500) + "..." : text;
    
    const systemPrompt = `Assistente de RH para extração rápida de dados de currículos.

REGRAS:
1. NOME: Nome completo, primeira letra maiúscula  
2. IDADE: Procure "X anos" OU calcule pela data nascimento (ano atual: ${currentYear}). Se não achar, use 0
3. EMAIL: Procure texto com "@". Se não encontrar, use ""
4. CONTATOS: Números telefone formatados (DD) 9XXXX-XXXX. Se não encontrar, use []
5. RESPOSTA: APENAS JSON válido e COMPLETO`;
    
    const userPrompt = `Extraia dados deste currículo:\n\n${truncatedText}`;
    
    const properties = { nome: { type: "STRING" } };
    const required = ["nome"];
    if (selectedFields.includes('idade')) { properties.idade = { type: "NUMBER" }; required.push('idade'); }
    if (selectedFields.includes('email')) { properties.email = { type: "STRING" }; required.push('email'); }
    if (selectedFields.includes('contatos')) { properties.contatos = { type: "ARRAY", items: { type: "STRING" } }; required.push('contatos'); }

    const payload = {
        contents: [{ parts: [{ text: userPrompt }] }],
        systemInstruction: { parts: [{ text: systemPrompt }] },
        generationConfig: {
            responseMimeType: "application/json",
            responseSchema: { type: "OBJECT", properties, required },
            maxOutputTokens: 500,
            temperature: 0
        }
    };
    
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 4000);
        
        const response = await fetch(apiUrl, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload),
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (!response.ok) {
            const errorText = await response.text().catch(() => 'Erro desconhecido');
            throw new Error(`API ${response.status}: ${errorText}`);
        }
        
        const responseText = await response.text();
        const result = JSON.parse(responseText);
        const candidate = result.candidates?.[0];
        
        if (candidate?.content?.parts?.[0]?.text) {
            const extractedText = candidate.content.parts[0].text.trim();
            
            try {
                const parsedData = JSON.parse(extractedText);
                return { success: true, data: parsedData };
            } catch (parseError) {
                // Tenta corrigir JSON truncado
                let correctedJson = extractedText.replace(/[,:]\s*$/, '');
                if (!correctedJson.trim().endsWith('}')) {
                    correctedJson += '}';
                }
                
                try {
                    const parsedData = JSON.parse(correctedJson);
                    return { success: true, data: parsedData };
                } catch (secondError) {
                    throw new Error(`JSON irrecuperável: ${parseError.message}`);
                }
            }
        }
        
        throw new Error('Resposta da API sem conteúdo válido');
        
    } catch (error) {
        if (error.name === 'AbortError') {
            return { success: false, error: 'Timeout na API Gemini' };
        }
        return { success: false, error: error.message };
    }
}

function extractWithFallback(text, selectedFields, jobId) {
    const result = { nome: 'Nome não encontrado' };
    
    try {
        // Nome
        const nomePatterns = [
            /^([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-zàáâãäéêëíîïóôõöúûüç]+(?:\s+(?:de|da|do|dos|das)?\s*[A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-zàáâãäéêëíîïóôõöúûüç]+)+)/m,
            /Nome[:\s]+([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][^\n\r]{10,60})/i
        ];
        
        for (const pattern of nomePatterns) {
            const match = text.match(pattern);
            if (match && match[1]) {
                result.nome = match[1].trim().replace(/\s+/g, ' ');
                break;
            }
        }
        
        // Idade
        if (selectedFields.includes('idade')) {
            const idadeMatch = text.match(/(\d{1,2})\s+anos?\b/i);
            if (idadeMatch) {
                const idade = parseInt(idadeMatch[1]);
                if (idade >= 16 && idade <= 80) {
                    result.idade = idade;
                }
            }
            if (!result.idade) result.idade = 0;
        }
        
        // Email
        if (selectedFields.includes('email')) {
            const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            if (emailMatch) {
                result.email = emailMatch[1].toLowerCase();
            }
        }
        
        // Contatos
        if (selectedFields.includes('contatos')) {
            const phoneMatches = text.match(/(?:\((\d{2})\)\s*)?(?:9?\s*)?(\d{4,5})[\s\-]?(\d{4})/g);
            if (phoneMatches) {
                const contatos = phoneMatches.map(match => {
                    const parts = match.match(/(?:\((\d{2})\)\s*)?(?:9?\s*)?(\d{4,5})[\s\-]?(\d{4})/);
                    const ddd = parts[1] || '00';
                    const numero = parts[2] + parts[3];
                    
                    return numero.length === 9 
                        ? `(${ddd}) 9 ${numero.substring(1, 5)}-${numero.substring(5)}`
                        : `(${ddd}) ${numero.substring(0, 4)}-${numero.substring(4)}`;
                }).slice(0, 3); // Máximo 3 contatos
                
                if (contatos.length > 0) {
                    result.contatos = contatos;
                }
            }
        }
        
        return { success: true, data: result };
        
    } catch (error) {
        return { 
            success: true,
            data: { nome: `ERRO: Falha na extração` }
        };
    }
}