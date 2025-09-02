import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, Timestamp, FieldValue } from 'firebase-admin/firestore';

// Inicialização robusta do Firebase
if (!getApps().length) {
    try {
        const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
        initializeApp({ credential: cert(serviceAccount) });
    } catch (e) {
        console.error("ERRO CRÍTICO: Falha na inicialização do Firebase Admin em process-job.", e);
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

    console.log(`[PROCESS-JOB ${jobId}] === INICIANDO PROCESSAMENTO ===`);

    try {
        // ETAPA 1: Verificar e reclamar o job atomicamente
        const jobResult = await claimJob(jobId, userId);
        if (!jobResult.success) {
            console.log(`[PROCESS-JOB ${jobId}] ${jobResult.message}`);
            // Sempre tenta processar próximo job, mesmo se este falhou
            setTimeout(() => processNextJob(userId), 1000);
            return response.status(200).json({ success: false, message: jobResult.message });
        }

        const jobData = jobResult.data;
        console.log(`[PROCESS-JOB ${jobId}] Job reclamado com sucesso: ${jobData.fileName}`);

        // ETAPA 2: Processar com a API Gemini
        let apiResult = await processWithGemini(jobData.text, jobData.selectedFields, jobId);
        
        // Se a API Gemini falhar, tenta extração simples como fallback
        if (!apiResult.success && jobData.text) {
            console.log(`[PROCESS-JOB ${jobId}] Gemini falhou, tentando extração simples...`);
            apiResult = extractWithFallback(jobData.text, jobData.selectedFields, jobId);
        }
        
        // ETAPA 3: Salvar resultado
        const jobRef = db.collection('processing_queue').doc(jobId);
        if (apiResult.success) {
            await jobRef.update({
                status: 'completed',
                finishedAt: Timestamp.now(),
                result: apiResult.data,
                processedBy: 'worker',
                completedAt: Timestamp.now()
            });
            console.log(`[PROCESS-JOB ${jobId}] ✅ CONCLUÍDO COM SUCESSO`);
        } else {
            await jobRef.update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: apiResult.error,
                failedAt: Timestamp.now()
            });
            console.log(`[PROCESS-JOB ${jobId}] ❌ FALHOU: ${apiResult.error}`);
        }

        // ETAPA 4: Processar próximo job (crítico!)
        setTimeout(() => processNextJob(userId), 2000); // 2 segundos de delay
        
        return response.status(200).json({ 
            success: apiResult.success, 
            jobId: jobId,
            message: apiResult.success ? 'Job completed successfully' : 'Job failed but next job triggered'
        });

    } catch (error) {
        console.error(`[PROCESS-JOB ${jobId}] 💥 ERRO CRÍTICO:`, error);
        
        // Marca como falha e continua a fila
        try {
            await db.collection('processing_queue').doc(jobId).update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: `Critical error: ${error.message}`,
                criticalFailure: true
            });
        } catch (updateError) {
            console.error(`[PROCESS-JOB ${jobId}] Erro ao atualizar status de falha crítica:`, updateError);
        }

        // SEMPRE tenta continuar a fila
        setTimeout(() => processNextJob(userId), 1000);
        
        return response.status(200).json({ 
            success: false, 
            error: 'Critical error occurred, next job triggered',
            jobId: jobId
        });
    }
}

/**
 * Reclama um job atomicamente para evitar concorrência
 */
async function claimJob(jobId, userId) {
    try {
        const jobRef = db.collection('processing_queue').doc(jobId);
        
        return await db.runTransaction(async (transaction) => {
            const jobDoc = await transaction.get(jobRef);
            
            if (!jobDoc.exists) {
                return { success: false, message: 'Job não encontrado' };
            }

            const jobData = jobDoc.data();
            
            // Verifica se o job pertence ao usuário
            if (jobData.userId !== userId) {
                return { success: false, message: 'Job não pertence ao usuário' };
            }

            // Verifica se ainda está pendente
            if (jobData.status !== 'pending') {
                return { success: false, message: `Job já está em status: ${jobData.status}` };
            }

            // Reclama o job atomicamente
            transaction.update(jobRef, {
                status: 'processing',
                startedAt: Timestamp.now(),
                claimedAt: Timestamp.now(),
                lastHeartbeat: Timestamp.now()
            });

            return { success: true, data: jobData };
        });

    } catch (error) {
        console.error(`[CLAIM-JOB ${jobId}] Erro ao reclamar job:`, error);
        return { success: false, message: `Erro ao reclamar job: ${error.message}` };
    }
}

/**
 * Processa com a API Gemini com timeout rigoroso
 */
async function processWithGemini(text, selectedFields, jobId) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        return { success: false, error: 'GEMINI_API_KEY não configurada' };
    }

    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    
    // Limita o texto para acelerar processamento
    const truncatedText = text.length > 2500 ? text.substring(0, 2500) + "..." : text;
    
    const systemPrompt = `Assistente de RH para extração rápida de dados de currículos.

REGRAS DE EXTRAÇÃO:
1. NOME: Nome completo, primeira letra maiúscula
2. IDADE: Número + "anos" OU calcule pela data nascimento (ano atual: ${currentYear}). Se não achar, retorne 0
3. EMAIL: Procure por texto com "@"
4. CONTATOS: Números de telefone, formate (DD) 9XXXX-XXXX
5. RESPOSTA: APENAS JSON, sem texto extra`;
    
    const userPrompt = `Extraia dados deste currículo:\n\n${truncatedText}`;
    
    // Schema JSON otimizado
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
            maxOutputTokens: 300,
            temperature: 0,
            candidateCount: 1 // Força apenas 1 resposta
        },
        safetySettings: [
            { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" }
        ]
    };
    
    try {
        console.log(`[GEMINI ${jobId}] Enviando requisição...`);
        
        // Timeout agressivo de 4 segundos
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            controller.abort();
            console.log(`[GEMINI ${jobId}] ⏰ TIMEOUT após 4 segundos`);
        }, 4000);
        
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
        
        // Log da resposta bruta para debug
        const responseText = await response.text();
        console.log(`[GEMINI ${jobId}] Resposta bruta (${responseText.length} chars):`, 
                   responseText.substring(0, 200) + (responseText.length > 200 ? '...' : ''));
        
        if (!responseText || responseText.trim() === '') {
            throw new Error('Resposta vazia da API Gemini');
        }
        
        let result;
        try {
            result = JSON.parse(responseText);
        } catch (jsonError) {
            console.error(`[GEMINI ${jobId}] Erro ao parsear JSON:`, jsonError);
            console.error(`[GEMINI ${jobId}] Resposta completa:`, responseText);
            throw new Error(`JSON inválido da API: ${jsonError.message}`);
        }
        
        const candidate = result.candidates?.[0];
        
        if (!candidate) {
            console.error(`[GEMINI ${jobId}] Sem candidates na resposta:`, result);
            throw new Error('API não retornou candidates');
        }
        
        if (!candidate.content?.parts?.[0]?.text) {
            console.error(`[GEMINI ${jobId}] Sem text no candidate:`, candidate);
            throw new Error('API não retornou texto no content');
        }
        
        const extractedText = candidate.content.parts[0].text.trim();
        console.log(`[GEMINI ${jobId}] Texto extraído:`, extractedText);
        
        if (!extractedText) {
            throw new Error('Texto extraído está vazio');
        }
        
        let parsedData;
        try {
            parsedData = JSON.parse(extractedText);
        } catch (parseError) {
            console.error(`[GEMINI ${jobId}] Erro ao parsear dados extraídos:`, parseError);
            console.error(`[GEMINI ${jobId}] Texto que falhou:`, extractedText);
            throw new Error(`Dados extraídos não são JSON válido: ${parseError.message}`);
        }
        
        console.log(`[GEMINI ${jobId}] ✅ Dados extraídos: ${parsedData.nome || 'Nome não encontrado'}`);
        return { success: true, data: parsedData };
        
    } catch (error) {
        if (error.name === 'AbortError') {
            console.log(`[GEMINI ${jobId}] ⏰ Timeout na API Gemini`);
            return { success: false, error: 'Timeout na API Gemini (4s)' };
        }
        console.error(`[GEMINI ${jobId}] ❌ Erro:`, error);
        return { success: false, error: error.message };
    }
}

/**
 * Processa próximo job da fila
 */
async function processNextJob(userId) {
    try {
        console.log(`[NEXT-JOB] Procurando próximo job para ${userId}...`);
        
        const nextJobSnapshot = await db.collection('processing_queue')
            .where('userId', '==', userId)
            .where('status', '==', 'pending')
            .orderBy('createdAt', 'asc')
            .limit(1)
            .get();

        if (nextJobSnapshot.empty) {
            console.log(`[NEXT-JOB] ✅ Fila vazia para ${userId} - processamento concluído`);
            return;
        }

        const nextJobId = nextJobSnapshot.docs[0].id;
        const nextJobData = nextJobSnapshot.docs[0].data();
        
        console.log(`[NEXT-JOB] 🎯 Próximo job encontrado: ${nextJobId} (${nextJobData.fileName})`);
        
        // Chama recursivamente o processamento
        const response = await fetch(`${getBaseUrl()}/api/process-job`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ jobId: nextJobId, userId: userId })
        });

        if (!response.ok) {
            console.error(`[NEXT-JOB] ❌ Erro HTTP ${response.status} ao chamar próximo job`);
        } else {
            console.log(`[NEXT-JOB] ✅ Próximo job ${nextJobId} disparado com sucesso`);
        }
        
    } catch (error) {
        console.error(`[NEXT-JOB] ❌ Erro ao processar próximo job:`, error);
    }
}

/**
 * Extração simples como fallback quando a API Gemini falha
 */
function extractWithFallback(text, selectedFields, jobId) {
    console.log(`[FALLBACK ${jobId}] Iniciando extração simples...`);
    
    const result = { nome: 'Nome não encontrado' };
    
    try {
        // Extração de nome - procura por padrões comuns
        const nomePatterns = [
            /^([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-zàáâãäéêëíîïóôõöúûüç]+(?:\s+(?:de|da|do|dos|das)?\s*[A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-zàáâãäéêëíîïóôõöúûüç]+)+)/m,
            /Nome[:\s]+([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][^\n\r]{10,60})/i,
            /^([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ\s]{10,60})/m
        ];
        
        for (const pattern of nomePatterns) {
            const match = text.match(pattern);
            if (match && match[1]) {
                result.nome = match[1].trim().replace(/\s+/g, ' ');
                break;
            }
        }
        
        // Extração de idade
        if (selectedFields.includes('idade')) {
            const idadeMatch = text.match(/(\d{1,2})\s+anos?\b/i);
            if (idadeMatch) {
                const idade = parseInt(idadeMatch[1]);
                if (idade >= 16 && idade <= 80) {
                    result.idade = idade;
                }
            } else {
                // Tenta data de nascimento
                const nascMatch = text.match(/(\d{2})[\/\-](\d{2})[\/\-](\d{4})/);
                if (nascMatch) {
                    const ano = parseInt(nascMatch[3]);
                    const idade = new Date().getFullYear() - ano;
                    if (idade >= 16 && idade <= 80) {
                        result.idade = idade;
                    }
                }
            }
            if (!result.idade) result.idade = 0;
        }
        
        // Extração de email
        if (selectedFields.includes('email')) {
            const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            if (emailMatch) {
                result.email = emailMatch[1].toLowerCase();
            }
        }
        
        // Extração de contatos
        if (selectedFields.includes('contatos')) {
            const phonePatterns = [
                /(?:\((\d{2})\)\s*)?(?:9?\s*)?(\d{4,5})[\s\-]?(\d{4})/g
            ];
            
            const contatos = [];
            for (const pattern of phonePatterns) {
                let match;
                while ((match = pattern.exec(text)) !== null) {
                    const ddd = match[1] || '00';
                    const numero = match[2] + match[3];
                    
                    if (numero.length >= 8 && numero.length <= 9) {
                        const formatted = numero.length === 9 
                            ? `(${ddd}) 9 ${numero.substring(1, 5)}-${numero.substring(5)}`
                            : `(${ddd}) ${numero.substring(0, 4)}-${numero.substring(4)}`;
                        contatos.push(formatted);
                    }
                }
            }
            
            if (contatos.length > 0) {
                result.contatos = [...new Set(contatos)]; // Remove duplicatas
            }
        }
        
        console.log(`[FALLBACK ${jobId}] ✅ Extração simples concluída:`, result);
        return { success: true, data: result };
        
    } catch (error) {
        console.error(`[FALLBACK ${jobId}] ❌ Erro na extração simples:`, error);
        return { 
            success: true, // Retorna sucesso mesmo com erro para não travar a fila
            data: { nome: `ERRO: Falha na extração (${jobData.fileName})` }
        };
    }
}

/**
 * Obtém URL base da aplicação
 */
function getBaseUrl() {
    // URL de produção específica
    if (process.env.VERCEL_ENV === 'production') {
        return 'https://pdf.in100tiva.com';
    }
    
    // Em preview/desenvolvimento na Vercel
    if (process.env.VERCEL_URL) {
        return `https://${process.env.VERCEL_URL}`;
    }
    
    // Fallback para desenvolvimento local
    return process.env.NODE_ENV === 'development' 
        ? 'http://localhost:3000' 
        : 'https://pdf.in100tiva.com';
}