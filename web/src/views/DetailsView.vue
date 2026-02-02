<script setup>
import { ref, onMounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import api from '../services/api';

const route = useRoute();
const router = useRouter();
const operadora = ref(null);
const historico = ref([]);
const loading = ref(true);

onMounted(async () => {
    const cnpj = route.params.cnpj;
    try {
        const [opRes, histRes] = await Promise.all([
            api.get(`/operadoras/${cnpj}`),
            api.get(`/operadoras/${cnpj}/despesas`)
        ]);
        operadora.value = opRes.data;
        historico.value = histRes.data;
    } catch (err) {
        console.error(err);
        alert("Erro ao carregar dados.");
    } finally {
        loading.value = false;
    }
});

const formatMoeda = (val) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);
</script>

<template>
    <div class="container">
        <button class="btn-voltar" @click="router.back()">← Voltar</button>

        <div v-if="loading" style="text-align: center; margin-top: 50px;">Carregando detalhes...</div>

        <div v-else-if="operadora">
            <h2>{{ operadora.razao_social }}</h2>
            
            <div class="info-card">
                <p><strong>CNPJ:</strong> {{ operadora.cnpj }}</p>
                <p><strong>Registro ANS:</strong> {{ operadora.registro_ans }}</p>
                <p><strong>Endereço:</strong> 
                    <span v-if="operadora.endereco">
                        {{ operadora.endereco.logradouro }} - {{ operadora.endereco.uf }}
                    </span>
                    <span v-else>Endereço não informado</span>
                </p>
            </div>

            <h3 style="margin-top: 30px;">Histórico de Despesas</h3>
            
            <div class="history-card">
                <table>
                    <thead>
                        <tr>
                            <th>Ano / Trimestre</th>
                            <th>Valor da Despesa</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr v-for="h in historico" :key="h.data_evento || h.trimestre">
                            <td>{{ h.ano }} / {{ h.trimestre }}</td>
                            <td>{{ formatMoeda(h.valor_despesa) }}</td>
                        </tr>
                        <tr v-if="historico.length === 0">
                            <td colspan="2" style="text-align: center; padding: 20px; color: #999;">
                                Nenhuma despesa registrada para esta operadora.
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</template>

<style scoped>
.container { 
    max-width: 900px; 
    margin: 0 auto; 
    padding: 40px 20px; 
}

/* Botão Voltar (Estilo secundário) */
.btn-voltar {
    background-color: white;
    color: #4b5563;
    border: 1px solid #d1d5db;
    margin-bottom: 20px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
.btn-voltar:hover {
    background-color: #f9fafb;
    border-color: #9ca3af;
    transform: none;
}

h2 {
    font-size: 1.8rem;
    color: #111827;
    margin-bottom: 20px;
}

/* Cartão de Informações */
.info-card {
    background: white;
    border-radius: 12px;
    padding: 30px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    border: 1px solid #e5e7eb;
}

.info-card p {
    margin: 12px 0;
    font-size: 1.05rem;
    color: #374151;
    line-height: 1.6;
    border-bottom: 1px dashed #e5e7eb;
    padding-bottom: 8px;
}
.info-card p:last-child { border-bottom: none; }
.info-card strong { color: #1f2937; margin-right: 8px; }

/* Cartão da Tabela */
.history-card {
    background: white;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    border: 1px solid #e5e7eb;
}
</style>