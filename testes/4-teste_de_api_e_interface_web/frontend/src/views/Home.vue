<template>
  <div class="container">
    <!-- Estatísticas -->
    <div v-if="estatisticas" class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Total de Despesas</div>
        <div class="stat-value">{{ formatarMoeda(estatisticas.total_despesas) }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Média de Despesas</div>
        <div class="stat-value">{{ formatarMoeda(estatisticas.media_despesas) }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total de Operadoras</div>
        <div class="stat-value">{{ totalOperadoras }}</div>
      </div>
    </div>

    <!-- Gráfico de Despesas por UF -->
    <div v-if="estatisticas" class="card">
      <h2 class="card-title">Distribuição de Despesas por UF</h2>
      <GraficoDespesasUf :dados="estatisticas.despesas_por_uf" />
    </div>

    <!-- Top 5 Operadoras -->
    <div v-if="estatisticas" class="card">
      <h2 class="card-title">Top 5 Operadoras com Maiores Despesas</h2>
      <TabelaTop5 :operadoras="estatisticas.top_5_operadoras" />
    </div>

    <!-- Busca e Tabela de Operadoras -->
    <div class="card">
      <h2 class="card-title">Lista de Operadoras</h2>
      
      <div class="search-box">
        <input
          v-model="busca"
          type="text"
          placeholder="Buscar por razão social ou CNPJ..."
          class="input"
          @input="debounceSearch"
        />
        <button @click="buscarOperadoras" class="btn btn-primary">
          Buscar
        </button>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="loading">
        Carregando operadoras
      </div>

      <!-- Erro -->
      <div v-else-if="erro" class="error">
        {{ erro }}
      </div>

      <!-- Tabela -->
      <div v-else-if="operadoras.length > 0">
        <TabelaOperadoras :operadoras="operadoras" />
        
        <Paginacao
          :pagina-atual="paginaAtual"
          :total="totalOperadoras"
          :limite="limite"
          @mudar-pagina="mudarPagina"
        />
      </div>

      <!-- Vazio -->
      <div v-else class="empty">
        Nenhuma operadora encontrada
      </div>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue'
import { operadorasService } from '../services/operadoras'
import TabelaOperadoras from '../components/TabelaOperadoras.vue'
import Paginacao from '../components/Paginacao.vue'
import GraficoDespesasUf from '../components/GraficoDespesasUf.vue'
import TabelaTop5 from '../components/TabelaTop5.vue'

export default {
  name: 'Home',
  components: {
    TabelaOperadoras,
    Paginacao,
    GraficoDespesasUf,
    TabelaTop5
  },
  setup() {
    const operadoras = ref([])
    const estatisticas = ref(null)
    const loading = ref(false)
    const erro = ref(null)
    const busca = ref('')
    const paginaAtual = ref(1)
    const totalOperadoras = ref(0)
    const limite = ref(10)
    let timeoutId = null

    const carregarOperadoras = async () => {
      loading.value = true
      erro.value = null
      
      try {
        const data = await operadorasService.listar(
          paginaAtual.value,
          limite.value,
          busca.value
        )
        operadoras.value = data.data
        totalOperadoras.value = data.total
      } catch (err) {
        erro.value = err.message
      } finally {
        loading.value = false
      }
    }

    const carregarEstatisticas = async () => {
      try {
        estatisticas.value = await operadorasService.obterEstatisticas()
      } catch (err) {
        console.error('Erro ao carregar estatísticas:', err)
      }
    }

    const buscarOperadoras = () => {
      paginaAtual.value = 1
      carregarOperadoras()
    }

    const debounceSearch = () => {
      clearTimeout(timeoutId)
      timeoutId = setTimeout(() => {
        buscarOperadoras()
      }, 500)
    }

    const mudarPagina = (novaPagina) => {
      paginaAtual.value = novaPagina
      carregarOperadoras()
    }

    const formatarMoeda = (valor) => {
      return new Intl.NumberFormat('pt-BR', {
        style: 'currency',
        currency: 'BRL'
      }).format(valor)
    }

    onMounted(() => {
      carregarOperadoras()
      carregarEstatisticas()
    })

    return {
      operadoras,
      estatisticas,
      loading,
      erro,
      busca,
      paginaAtual,
      totalOperadoras,
      limite,
      buscarOperadoras,
      debounceSearch,
      mudarPagina,
      formatarMoeda
    }
  }
}
</script>
