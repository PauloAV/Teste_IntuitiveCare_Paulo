<script setup>
import { ref, onMounted, watch } from 'vue';
import api from '../services/api';
import { useRouter } from 'vue-router';
import { Bar } from 'vue-chartjs';
import { Chart as ChartJS, Title, Tooltip, Legend, BarElement, CategoryScale, LinearScale } from 'chart.js';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

const router = useRouter();
const operadoras = ref([]);
const meta = ref({ page: 1, total_pages: 1 });
const search = ref('');
const loading = ref(false);
const chartData = ref(null);
const chartOptions = { responsive: true, maintainAspectRatio: false };

const fetchOperadoras = async (page = 1) => {
    loading.value = true;
    try {
        const res = await api.get('/operadoras', { 
            params: { page, limit: 10, termo: search.value } 
        });
        operadoras.value = res.data.data;
        meta.value = res.data.meta;
    } catch (err) {
        alert("Erro ao carregar dados. Tente novamente.");
    } finally {
        loading.value = false;
    }
};

const fetchChart = async () => {
    try {
        const res = await api.get('/estatisticas');
        const dadosUF = res.data.distribuicao_uf;
        
        if (dadosUF) {
             chartData.value = {
                labels: dadosUF.map(d => d.uf),
                datasets: [{
                    label: 'Despesas por UF (R$)',
                    backgroundColor: '#2563eb', // Azul do tema
                    data: dadosUF.map(d => d.total)
                }]
            };
        }
    } catch (err) {
        alert("Erro ao carregar gráfico. Tente novamente.");
    }
};

const goToDetails = (cnpj) => router.push(`/operadora/${cnpj}`);

let timer = null;
watch(search, () => {
    clearTimeout(timer);
    timer = setTimeout(() => fetchOperadoras(1), 500);
});

onMounted(() => {
    fetchOperadoras();
    fetchChart();
});
</script>

<template>
    <div class="container">
        <h1>Dashboard ANS</h1>
        
        <div class="chart-box card-box" v-if="chartData">
            <h3>Distribuição de Despesas por UF</h3>
            <div style="height: 300px;">
                <Bar :data="chartData" :options="chartOptions" />
            </div>
        </div>

        <div class="controls">
            <input v-model="search" type="text" placeholder="🔍 Buscar Operadora por Nome..." />
        </div>

        <div class="table-responsive card-box" v-if="!loading">
            <table>
                <thead>
                    <tr>
                        <th>Razão Social</th>
                        <th>CNPJ</th>
                        <th style="text-align: right;">Ação</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="op in operadoras" :key="op.cnpj">
                        <td>{{ op.razao_social }}</td>
                        <td>{{ op.cnpj }}</td>
                        <td style="text-align: right;">
                            <button @click="goToDetails(op.cnpj)">Detalhes</button>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <p v-else style="text-align: center; color: #666;">Carregando dados...</p>

        <div class="pagination">
            <button :disabled="meta.page <= 1" @click="fetchOperadoras(meta.page - 1)">Anterior</button>
            <span>Página {{ meta.page }} de {{ meta.total_pages }}</span>
            <button :disabled="meta.page >= meta.total_pages" @click="fetchOperadoras(meta.page + 1)">Próxima</button>
        </div>
    </div>
</template>

<style scoped>
.container { 
    max-width: 1100px; 
    margin: 0 auto; 
    padding: 40px 20px; 
}

h1 {
    text-align: center;
    margin-bottom: 40px;
    font-size: 2.2rem;
    color: #1e293b;
}

.chart-box h3 {
    margin-top: 0;
    margin-bottom: 20px;
    font-size: 1.1rem;
    color: #64748b;
}

.controls {
    margin-bottom: 20px;
}

.table-responsive {
    overflow-x: auto;
    padding: 0; /* Remove padding do card para a tabela encostar nas bordas */
    overflow: hidden; /* Garante bordas arredondadas */
}

.pagination {
    margin-top: 20px;
    display: flex;
    gap: 15px;
    justify-content: center;
    align-items: center;
    color: #6b7280;
}
</style>