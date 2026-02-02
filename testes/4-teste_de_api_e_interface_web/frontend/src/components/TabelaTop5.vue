<template>
  <div class="table-container">
    <table>
      <thead>
        <tr>
          <th>Posição</th>
          <th>Razão Social</th>
          <th>UF</th>
          <th>Total de Despesas</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(operadora, index) in operadoras" :key="index">
          <td>
            <strong>{{ index + 1 }}º</strong>
          </td>
          <td>{{ operadora.razao_social }}</td>
          <td>{{ operadora.uf || 'N/A' }}</td>
          <td>{{ formatarMoeda(operadora.total_despesas) }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  name: 'TabelaTop5',
  props: {
    operadoras: {
      type: Array,
      required: true
    }
  },
  setup() {
    const formatarMoeda = (valor) => {
      return new Intl.NumberFormat('pt-BR', {
        style: 'currency',
        currency: 'BRL'
      }).format(valor)
    }

    return {
      formatarMoeda
    }
  }
}
</script>
