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
          <div class="table-container">
            <table>
              <thead>
                <tr>
                  <th>Tipo</th>
                  <th>UF</th>
                  <th>Total de Despesas</th>
                  <th>Média por Trimestre</th>
                  <th>Desvio Padrão</th>
                  <th>Registros</th>
                  <th>Trimestres</th>
                  <th>Anos</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(despesa, index) in despesas" :key="index">
                  <td>
                    <span :class="badgeTipoDeducao(despesa.tipo_deducao)">
                      {{ despesa.tipo_deducao }}
                    </span>
                  </td>
                  <td>{{ despesa.uf || 'N/A' }}</td>
                  <td>{{ formatarMoeda(despesa.total_despesas) }}</td>
                  <td>{{ formatarMoeda(despesa.media_despesas_trimestre) }}</td>
                  <td>{{ formatarMoeda(despesa.desvio_padrao_despesas) }}</td>
                  <td>{{ despesa.qtd_registros }}</td>
                  <td>{{ despesa.qtd_trimestres }}</td>
                  <td>{{ despesa.qtd_anos }}</td>
                </tr>
              </tbody>
            </table>
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
import { ref, onMounted } from 'vue'
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

    onMounted(() => {
      carregarOperadora()
      carregarDespesas()
    })

    return {
      operadora,
      despesas,
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
