/**
 * Endpoint otimizado para processar jobs em lote na Vercel gratuita
 * Foca em eficiência e reliability para 100-200 CVs
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

    const startTime = Date.now();
    console.log(`[PROCESS-SINGLE ${jobId}] === INICIANDO ===`);

    try {
        // ETAPA 1: Reclama o job atomicamente
        const jobResult = await claimJob(jobId, userId);
        if (!jobResult.success) {
            console.log(`[PROCESS-SINGLE ${jobId}] ${jobResult.message}`);
            return response.status(200).json({ 
                success: false, 
                message: jobResult.message,
                processingTime: Date.now() - startTime
            });
        }

        const jobData = jobResult.data;
        console.log(`[PROCESS-SINGLE ${jobId}] Processando: ${jobData.fileName} (${jobData.text?.length || 0} chars)`);

        // ETAPA 2: Processar com estratégias múltiplas
        let processingResult;
        
        // Tenta extração rápida primeiro (para economizar cota da API)
        if (shouldUseQuickExtraction(jobData.text)) {
            console.log(`[PROCESS-SINGLE ${jobId}] Usando extração rápida`);
            processingResult = extractWithFallback(jobData.text, jobData.selectedFields, jobId);
        } else {
            // Usa Gemini para casos mais complexos
            console.log(`[PROCESS-SINGLE ${jobId}] Usando Gemini API`);
            processingResult = await processWithGeminiOptimized(jobData.text, jobData.selectedFields, jobId);
            
            // Fallback se Gemini falhar
            if (!processingResult.success && jobData.text) {
                console.log(`[PROCESS-SINGLE ${jobId}] Gemini falhou, usando fallback`);
                processingResult = extractWithFallback(jobData.text, jobData.selectedFields, jobId);
            }
        }
        
        // ETAPA 3: Salvar resultado otimizado
        const jobRef = db.collection('processing_queue').doc(jobId);
        const processingTime = Date.now() - startTime;
        
        if (processingResult.success) {
            await jobRef.update({
                status: 'completed',
                finishedAt: Timestamp.now(),
                result: processingResult.data,
                processingTimeMs: processingTime,
                extractionMethod: processingResult.method || 'gemini'
            });
            console.log(`[PROCESS-SINGLE ${jobId}] ✅ CONCLUÍDO em ${processingTime}ms`);
        } else {
            await jobRef.update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: processingResult.error,
                processingTimeMs: processingTime
            });
            console.log(`[PROCESS-SINGLE ${jobId}] ❌ FALHOU em ${processingTime}ms: ${processingResult.error}`);
        }

        return response.status(200).json({ 
            success: processingResult.success, 
            jobId: jobId,
            processingTime: processingTime,
            extractionMethod: processingResult.method || 'unknown'
        });

    } catch (error) {
        const processingTime = Date.now() - startTime;
        console.error(`[PROCESS-SINGLE ${jobId}] 💥 ERRO CRÍTICO em ${processingTime}ms:`, error);
        
        try {
            await db.collection('processing_queue').doc(jobId).update({
                status: 'failed',
                finishedAt: Timestamp.now(),
                error: `Critical error: ${error.message}`,
                processingTimeMs: processingTime,
                criticalFailure: true
            });
        } catch (updateError) {
            console.error(`[PROCESS-SINGLE ${jobId}] Erro ao marcar falha crítica:`, updateError);
        }

        return response.status(200).json({ 
            success: false, 
            error: error.message,
            jobId: jobId,
            processingTime: processingTime
        });
    }
}

/**
 * Determina se deve usar extração rápida baseado no conteúdo
 */
function shouldUseQuickExtraction(text) {
    if (!text || text.length < 200) return true;
    
    // Se tem padrões muito claros, usa extração rápida
    const hasName = /^[A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-záàâãäéêëíîïóôõöúûüç]+\s+[A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ]/m.test(text);
    const hasAge = /\d{1,2}\s+anos?\b/i.test(text);
    const hasEmail = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/.test(text);
    const hasPhone = /\(?\d{2}\)?[\s-]?9?\d{4,5}[\s-]?\d{4}/.test(text);
    
    // Se tem pelo menos 3 padrões claros, usa extração rápida
    const clearPatterns = [hasName, hasAge, hasEmail, hasPhone].filter(Boolean).length;
    return clearPatterns >= 3;
}

/**
 * Reclama job atomicamente
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
            
            if (jobData.userId !== userId) {
                return { success: false, message: 'Job não pertence ao usuário' };
            }

            if (jobData.status !== 'pending') {
                return { success: false, message: `Job já está em status: ${jobData.status}` };
            }

            transaction.update(jobRef, {
                status: 'processing',
                startedAt: Timestamp.now(),
                processingNode: 'vercel-single'
            });

            return { success: true, data: jobData };
        });

    } catch (error) {
        console.error(`[CLAIM-JOB ${jobId}] Erro:`, error);
        return { success: false, message: error.message };
    }
}

/**
 * Versão otimizada do processamento Gemini para lote
 */
async function processWithGeminiOptimized(text, selectedFields, jobId) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        return { success: false, error: 'GEMINI_API_KEY não configurada' };
    }

    // Usa o modelo mais rápido para processamento em lote
    const model = "gemini-2.5-flash-preview-05-20";
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const currentYear = new Date().getFullYear();
    
    // Trunca texto de forma mais agressiva para economia
    const maxLength = 1800; // Reduzido para acelerar
    const truncatedText = text.length > maxLength ? text.substring(0, maxLength) + "..." : text;
    
    // Prompt mais direto e eficiente
    const systemPrompt = `Extrator de CV ultra-rápido. Extraia APENAS os dados pedidos.

REGRAS SIMPLES:
- NOME: Nome completo da pessoa
- IDADE: Número + "anos" OU calcule (ano ${currentYear} - ano nascimento). Se não achar: 0  
- EMAIL: Primeiro email válido com @. Se não achar: ""
- CONTATOS: Até 2 telefones formato (XX) XXXXX-XXXX. Se não achar: []

RESPOSTA: Apenas JSON válido, sem explicação.`;
    
    // Schema simplificado
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

    const payload = {
        contents: [{ parts: [{ text: `CV:\n${truncatedText}` }] }],
        systemInstruction: { parts: [{ text: systemPrompt }] },
        generationConfig: {
            responseMimeType: "application/json",
            responseSchema: { type: "OBJECT", properties, required },
            maxOutputTokens: 300, // Reduzido para acelerar
            temperature: 0,
            candidateCount: 1
        },
        safetySettings: [
            { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" }
        ]
    };
    
    try {
        // Timeout mais curto para processamento em lote
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            controller.abort();
            console.log(`[GEMINI ${jobId}] ⏰ TIMEOUT após 3 segundos`);
        }, 3000);
        
        const response = await fetch(apiUrl, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload),
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (!response.ok) {
            const errorText = await response.text().catch(() => 'Erro desconhecido');
            throw new Error(`API ${response.status}: ${errorText.substring(0, 100)}`);
        }
        
        const responseText = await response.text();
        const result = JSON.parse(responseText);
        const candidate = result.candidates?.[0];
        
        if (candidate?.content?.parts?.[0]?.text) {
            const extractedText = candidate.content.parts[0].text.trim();
            
            try {
                const parsedData = JSON.parse(extractedText);
                console.log(`[GEMINI ${jobId}] ✅ Extraído: ${parsedData.nome || 'N/A'}`);
                return { success: true, data: parsedData, method: 'gemini' };
            } catch (parseError) {
                // Correção automática mais agressiva
                let correctedJson = extractedText
                    .replace(/[,:]\s*$/, '')
                    .replace(/^\s*\{?\s*/, '{')
                    .replace(/\s*\}?\s*$/, '}');
                
                try {
                    const parsedData = JSON.parse(correctedJson);
                    console.log(`[GEMINI ${jobId}] ✅ JSON corrigido: ${parsedData.nome || 'N/A'}`);
                    return { success: true, data: parsedData, method: 'gemini-corrected' };
                } catch (secondError) {
                    throw new Error(`JSON irrecuperável: ${parseError.message.substring(0, 50)}`);
                }
            }
        }
        
        throw new Error('Resposta da API sem conteúdo válido');
        
    } catch (error) {
        if (error.name === 'AbortError') {
            return { success: false, error: 'Timeout Gemini (3s)', method: 'gemini-timeout' };
        }
        console.error(`[GEMINI ${jobId}] ❌ Erro:`, error.message.substring(0, 100));
        return { success: false, error: error.message, method: 'gemini-error' };
    }
}

/**
 * Extração rápida otimizada para lote
 */
function extractWithFallback(text, selectedFields, jobId) {
    console.log(`[FALLBACK ${jobId}] Extração rápida iniciada`);
    
    const result = { nome: 'Nome não encontrado' };
    
    try {
        // === EXTRAÇÃO DE NOME (otimizada) ===
        const nomePatterns = [
            // Padrão mais comum: Nome próprio no início
            /^([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-záàâãäéêëíîïóôõöúûüç]+(?:\s+(?:de|da|do|dos|das)?\s*[A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][a-záàâãäéêëíîïóôõöúûüç]+)+)/m,
            // Após "Nome:"
            /Nome[:\s]+([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][^\n\r]{8,50})/i,
            // Primeira linha com nome próprio
            /^([A-ZÀÁÂÃÄÉÊËÍÎÏÓÔÕÖÚÛÜÇ][A-Za-záàâãäéêëíîïóôõöúûüç\s]{10,50})/m
        ];
        
        for (const pattern of nomePatterns) {
            const match = text.match(pattern);
            if (match && match[1]) {
                const nome = match[1].trim()
                    .replace(/\s+/g, ' ')
                    .replace(/[^\w\sÀ-ÿ]/g, '') // Remove caracteres especiais
                    .substring(0, 60); // Limita tamanho
                
                if (nome.length >= 8 && nome.split(' ').length >= 2) {
                    result.nome = nome;
                    break;
                }
            }
        }
        
        // === EXTRAÇÃO DE IDADE (otimizada) ===
        if (selectedFields.includes('idade')) {
            result.idade = 0;
            
            // Procura "X anos"
            const idadeMatch = text.match(/(\d{1,2})\s+anos?\b/i);
            if (idadeMatch) {
                const idade = parseInt(idadeMatch[1]);
                if (idade >= 16 && idade <= 80) {
                    result.idade = idade;
                }
            } else {
                // Procura data de nascimento
                const nascPatterns = [
                    /(\d{2})[\/\-](\d{2})[\/\-](\d{4})/g,
                    /(\d{4})[\/\-](\d{2})[\/\-](\d{2})/g
                ];
                
                for (const pattern of nascPatterns) {
                    const matches = [...text.matchAll(pattern)];
                    for (const match of matches) {
                        const ano = pattern.source.startsWith('(\\d{4}') 
                            ? parseInt(match[1]) 
                            : parseInt(match[3]);
                        
                        if (ano >= 1940 && ano <= 2010) {
                            const idade = new Date().getFullYear() - ano;
                            if (idade >= 16 && idade <= 80) {
                                result.idade = idade;
                                break;
                            }
                        }
                    }
                    if (result.idade > 0) break;
                }
            }
        }
        
        // === EXTRAÇÃO DE EMAIL (otimizada) ===
        if (selectedFields.includes('email')) {
            result.email = "";
            
            const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            if (emailMatch) {
                const email = emailMatch[1].toLowerCase().trim();
                // Valida se é um email real
                if (email.includes('.') && !email.includes('..') && email.length <= 50) {
                    result.email = email;
                }
            }
        }
        
        // === EXTRAÇÃO DE CONTATOS (otimizada) ===
        if (selectedFields.includes('contatos')) {
            result.contatos = [];
            
            // Padrões mais específicos para telefone brasileiro
            const phonePatterns = [
                /\((\d{2})\)\s*9?\s*(\d{4,5})[\s\-]?(\d{4})/g,
                /(\d{2})\s*9?\s*(\d{4,5})[\s\-]?(\d{4})/g
            ];
            
            const foundPhones = new Set(); // Evita duplicatas
            
            for (const pattern of phonePatterns) {
                let match;
                while ((match = pattern.exec(text)) !== null && foundPhones.size < 3) {
                    const ddd = match[1];
                    const numero = match[2] + match[3];
                    
                    // Valida DDD válido
                    const dddNum = parseInt(ddd);
                    if (dddNum >= 11 && dddNum <= 99) {
                        let formatted;
                        
                        if (numero.length === 9) {
                            // Celular
                            formatted = `(${ddd}) ${numero.substring(0, 5)}-${numero.substring(5)}`;
                        } else if (numero.length === 8) {
                            // Fixo
                            formatted = `(${ddd}) ${numero.substring(0, 4)}-${numero.substring(4)}`;
                        }
                        
                        if (formatted) {
                            foundPhones.add(formatted);
                        }
                    }
                }
            }
            
            if (foundPhones.size > 0) {
                result.contatos = Array.from(foundPhones);
            }
        }
        
        console.log(`[FALLBACK ${jobId}] ✅ Extraído: ${result.nome} (${selectedFields.join(', ')})`);
        return { success: true, data: result, method: 'fallback' };
        
    } catch (error) {
        console.error(`[FALLBACK ${jobId}] ❌ Erro:`, error);
        return { 
            success: true, // Sempre sucesso para não travar fila
            data: { nome: `ERRO: Falha na extração (${error.message.substring(0, 30)})` },
            method: 'fallback-error'
        };
    }
}