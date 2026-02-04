<template>
  <div class="container">
    <button @click="voltar" class="btn btn-secondary" style="margin-bottom: 1rem;">
      ← Voltar
    </button>

    <!-- Loading -->
    <div v-if="loading" class="loading">
      Carregando detalhes da operadora
    </div>

    <!-- Erro -->
    <div v-else-if="erro" class="error">
      {{ erro }}
    </div>

    <!-- Conteúdo -->
    <div v-else-if="operadora">
      <!-- Detalhes da Operadora -->
      <div class="card">
        <h2 class="card-title">Detalhes da Operadora</h2>
        <div class="detail-grid">
          <div class="detail-item">
            <div class="detail-label">Registro ANS</div>
            <div class="detail-value">{{ operadora.reg_ans }}</div>
          </div>
          <div class="detail-item">
            <div class="detail-label">CNPJ</div>
            <div class="detail-value">{{ formatarCnpj(operadora.cnpj) }}</div>
          </div>
          <div class="detail-item">
            <div class="detail-label">Razão Social</div>
            <div class="detail-value">{{ operadora.razao_social }}</div>
          </div>
          <div class="detail-item">
            <div class="detail-label">Modalidade</div>
            <div class="detail-value">{{ operadora.modalidade || 'N/A' }}</div>
          </div>
          <div class="detail-item">
            <div class="detail-label">UF</div>
            <div class="detail-value">{{ operadora.uf || 'N/A' }}</div>
          </div>
          <div class="detail-item">
            <div class="detail-label">Status</div>
            <div class="detail-value">
              <span :class="badgeClass(operadora.status)">
                {{ operadora.status }}
              </span>
            </div>
          </div>
        </div>
      </div>

      <!-- Histórico de Despesas -->
      <div class="card">
        <h2 class="card-title">Histórico de Despesas</h2>
        
        <div v-if="loadingDespesas" class="loading">
          Carregando despesas
        </div>
        
        <div v-else-if="erroDespesas" class="error">
          {{ erroDespesas }}
        </div>
        
        <div v-else-if="despesas.length > 0">
          <!-- Despesas SEM DEDUÇÃO -->
          <div v-if="despesasSemDeducao.length > 0" style="margin-bottom: 2rem;">
            <h3 style="margin-bottom: 1rem;">
              <span class="badge badge-warning">SEM DEDUÇÃO</span>
            </h3>
            <div class="table-container">
              <table>
                <thead>
                  <tr>
                    <th>Ano</th>
                    <th>Trimestre</th>
                    <th>Valor</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(despesa, index) in despesasSemDeducao" :key="'sem-' + index">
                    <td>{{ despesa.ano }}</td>
                    <td>{{ despesa.trimestre }}º Trimestre</td>
                    <td>{{ formatarMoeda(despesa.valor) }}</td>
                  </tr>
                  <tr class="total-row">
                    <td colspan="2"><strong>TOTAL</strong></td>
                    <td><strong>{{ formatarMoeda(totalSemDeducao) }}</strong></td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- Despesas COM DEDUÇÃO -->
          <div v-if="despesasComDeducao.length > 0">
            <h3 style="margin-bottom: 1rem;">
              <span class="badge badge-success">COM DEDUÇÃO</span>
            </h3>
            <div class="table-container">
              <table>
                <thead>
                  <tr>
                    <th>Ano</th>
                    <th>Trimestre</th>
                    <th>Valor</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(despesa, index) in despesasComDeducao" :key="'com-' + index">
                    <td>{{ despesa.ano }}</td>
                    <td>{{ despesa.trimestre }}º Trimestre</td>
                    <td>{{ formatarMoeda(despesa.valor) }}</td>
                  </tr>
                  <tr class="total-row">
                    <td colspan="2"><strong>TOTAL</strong></td>
                    <td><strong>{{ formatarMoeda(totalComDeducao) }}</strong></td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
        
        <div v-else class="empty">
          Nenhuma despesa encontrada para esta operadora
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { operadorasService } from '../services/operadoras'

export default {
  name: 'Detalhes',
  props: {
    cnpj: {
      type: String,
      required: true
    }
  },
  setup(props) {
    const router = useRouter()
    const operadora = ref(null)
    const despesas = ref([])
    const loading = ref(false)
    const erro = ref(null)
    const loadingDespesas = ref(false)
    const erroDespesas = ref(null)

    const carregarOperadora = async () => {
      loading.value = true
      erro.value = null
      
      try {
        operadora.value = await operadorasService.obterPorCnpj(props.cnpj)
      } catch (err) {
        erro.value = err.message
      } finally {
        loading.value = false
      }
    }

    const carregarDespesas = async () => {
      loadingDespesas.value = true
      erroDespesas.value = null
      
      try {
        despesas.value = await operadorasService.obterDespesas(props.cnpj)
      } catch (err) {
        erroDespesas.value = err.message
      } finally {
        loadingDespesas.value = false
      }
    }

    const voltar = () => {
      router.push('/')
    }

    const formatarCnpj = (cnpj) => {
      if (!cnpj) return ''
      return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/, '$1.$2.$3/$4-$5')
    }

    const formatarMoeda = (valor) => {
      return new Intl.NumberFormat('pt-BR', {
        style: 'currency',
        currency: 'BRL'
      }).format(valor)
    }

    const badgeClass = (status) => {
      const classes = {
        'ATIVA': 'badge badge-success',
        'CANCELADA': 'badge badge-danger',
        'SUSPENSA': 'badge badge-warning'
      }
      return classes[status] || 'badge'
    }

    const badgeTipoDeducao = (tipo) => {
      return tipo === 'SEM DEDUÇÃO' ? 'badge badge-warning' : 'badge badge-success'
    }

    const despesasSemDeducao = computed(() => {
      return despesas.value.filter(d => d.tipo_deducao === 'SEM DEDUÇÃO')
    })

    const despesasComDeducao = computed(() => {
      return despesas.value.filter(d => d.tipo_deducao === 'COM DEDUÇÃO')
    })

    const totalSemDeducao = computed(() => {
      return despesasSemDeducao.value.reduce((sum, d) => sum + (d.valor || 0), 0)
    })

    const totalComDeducao = computed(() => {
      return despesasComDeducao.value.reduce((sum, d) => sum + (d.valor || 0), 0)
    })

    onMounted(() => {
      carregarOperadora()
      carregarDespesas()
    })

    return {
      operadora,
      despesas,
      despesasSemDeducao,
      despesasComDeducao,
      totalSemDeducao,
      totalComDeducao,
      loading,
      erro,
      loadingDespesas,
      erroDespesas,
      voltar,
      formatarCnpj,
      formatarMoeda,
      badgeClass,
      badgeTipoDeducao
    }
  }
}
</script>

<style scoped>
.container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 2rem;
}

.loading, .error, .empty {
  text-align: center;
  padding: 2rem;
  margin: 1rem 0;
}

.error {
  color: #e74c3c;
}

.card {
  background: white;
  border-radius: 8px;
  padding: 2rem;
  margin-bottom: 2rem;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.card-title {
  margin-top: 0;
  margin-bottom: 1.5rem;
  color: #2c3e50;
  font-size: 1.5rem;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1.5rem;
}

.detail-item {
  display: flex;
  flex-direction: column;
}

.detail-label {
  font-size: 0.875rem;
  color: #7f8c8d;
  margin-bottom: 0.5rem;
}

.detail-value {
  font-size: 1rem;
  color: #2c3e50;
  font-weight: 500;
}

.table-container {
  overflow-x: auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 1rem;
}

thead {
  background-color: #f8f9fa;
}

th {
  padding: 1rem;
  text-align: left;
  font-weight: 600;
  color: #2c3e50;
  border-bottom: 2px solid #dee2e6;
}

td {
  padding: 1rem;
  border-bottom: 1px solid #dee2e6;
}

tr:hover {
  background-color: #f8f9fa;
}

tr.total-row {
  background-color: #e9ecef;
  font-weight: bold;
  border-top: 2px solid #dee2e6;
}

tr.total-row:hover {
  background-color: #e9ecef;
}

.badge {
  display: inline-block;
  padding: 0.25rem 0.75rem;
  border-radius: 4px;
  font-size: 0.875rem;
  font-weight: 500;
}

.badge-success {
  background-color: #d4edda;
  color: #155724;
}

.badge-danger {
  background-color: #f8d7da;
  color: #721c24;
}

.badge-warning {
  background-color: #fff3cd;
  color: #856404;
}

.btn {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 1rem;
  transition: background-color 0.2s;
}

.btn-secondary {
  background-color: #6c757d;
  color: white;
}

.btn-secondary:hover {
  background-color: #5a6268;
}
</style>
